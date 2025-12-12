# lfs_modular_v2.py
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import logging
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple

import pandas as pd
from rdflib import Graph

try:
    from SPARQLWrapper import SPARQLWrapper, JSON  # optional
except Exception:
    SPARQLWrapper = None
    JSON = None


# ----------------------------
# Types
# ----------------------------

TripleKey = str  # "S P O ." (single-line N-Triples fragment)


# ----------------------------
# Config
# ----------------------------

@dataclass(frozen=True)
class Config:
    summary_file: Path
    queries_file: Path
    output_dir: Path
    endpoint_url: Optional[str]
    percents: List[float]
    fold_seed: int
    test_fraction: float
    max_query_chars: int
    max_query_vars: int
    skip_substrings: Tuple[str, ...]
    metrics_out: Path


def parse_args() -> Config:
    p = argparse.ArgumentParser(description="Robust LFS summarizer + query coverage evaluation.")
    p.add_argument("summary_file", type=Path)
    p.add_argument("queries_file", type=Path)
    p.add_argument("--output-dir", type=Path, default=Path("lfs_out"))
    p.add_argument("--endpoint", type=str, default=None)
    p.add_argument("--percents", type=str, default="0.2,0.4,0.6,0.8,1.0")
    p.add_argument("--seed", type=int, default=2)
    p.add_argument("--test-fraction", type=float, default=0.2)
    p.add_argument("--max-query-chars", type=int, default=700)
    p.add_argument("--max-query-vars", type=int, default=25)
    p.add_argument("--metrics-out", type=Path, default=Path("output.wiki10.txt"))

    args = p.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    percents = [float(x.strip()) for x in args.percents.split(",") if x.strip()]
    skip_substrings = (
        "2023-01-31T01:09:43Z",
        "<<",
        "math",
        "<application/x-httpd-php>",
        "10766787-n",
        "1848831457",
    )

    return Config(
        summary_file=args.summary_file,
        queries_file=args.queries_file,
        output_dir=args.output_dir,
        endpoint_url=args.endpoint,
        percents=percents,
        fold_seed=args.seed,
        test_fraction=args.test_fraction,
        max_query_chars=args.max_query_chars,
        max_query_vars=args.max_query_vars,
        skip_substrings=skip_substrings,
        metrics_out=args.metrics_out,
    )


# ----------------------------
# Validation helpers
# ----------------------------

_URI_RE = re.compile(r"^<https?://[^\s>]+>$")
_BNODE_RE = re.compile(r"^_:[A-Za-z][A-Za-z0-9]*$")
# permissive literal: "..." optionally @lang or ^^<datatype>
_LITERAL_RE = re.compile(r"^\".*\"(?:@[a-zA-Z]+(?:-[a-zA-Z0-9]+)*)?(?:\^\^<https?://[^\s>]+>)?$")


def is_valid_nt(subject: str, predicate: str, obj: str) -> bool:
    if not (_URI_RE.match(subject) or _BNODE_RE.match(subject)):
        return False
    if not _URI_RE.match(predicate):
        return False
    if not (_URI_RE.match(obj) or _BNODE_RE.match(obj) or _LITERAL_RE.match(obj)):
        return False
    return True


def should_skip_line(line: str, skip_substrings: Sequence[str]) -> bool:
    return any(s in line for s in skip_substrings)


# ----------------------------
# Robust tokenization of N-Triples-ish fragments
# ----------------------------

_NT_TOKEN_RE = re.compile(
    r"""
    <[^>]*>                 # URIRef
    | _:[A-Za-z][A-Za-z0-9]* # blank node
    | "([^"\\]|\\.)*"        # quoted literal with escapes
      (?:@[a-zA-Z]+(?:-[a-zA-Z0-9]+)*)?
      (?:\^\^<[^>]*>)?
    """,
    re.VERBOSE,
)


def extract_nt_terms(fragment: str) -> List[str]:
    """
    Extracts RDF terms from a fragment using a regex that understands:
      - <...> URIs
      - _:bnode
      - "literal"@lang
      - "literal"^^<datatype>
    """
    return [m.group(0) for m in _NT_TOKEN_RE.finditer(fragment)]


def extract_triples_from_row(row: str, skip_substrings: Sequence[str]) -> List[Tuple[str, str, str]]:
    """
    Row contains one or more triple fragments separated by tabs (like original).
    Each fragment should contain at least 3 RDF terms (S P O).
    """
    if should_skip_line(row, skip_substrings):
        return []

    triples: List[Tuple[str, str, str]] = []
    for frag in row.split("\t"):
        frag = frag.strip()
        if not frag:
            continue
        terms = extract_nt_terms(frag)
        if len(terms) < 3:
            continue
        s, p, o = terms[0], terms[1], terms[2]
        if is_valid_nt(s, p, o):
            triples.append((s, p, o))
    return triples


def triple_key(s: str, p: str, o: str) -> TripleKey:
    return f"{s} {p} {o} ."


# ----------------------------
# Build frequency + co-occurrence (no query-index coupling)
# ----------------------------

def build_counts_and_cooccurrence(
    rows: Iterable[str],
    skip_substrings: Sequence[str],
) -> Tuple[Dict[TripleKey, int], Dict[TripleKey, Set[TripleKey]], int]:
    """
    - trips_freq: how often each triple appears
    - cooc: for each triple, which other triples appeared in the same row
    """
    trips_freq: Dict[TripleKey, int] = {}
    cooc: Dict[TripleKey, Set[TripleKey]] = {}
    gross = 0

    for row in rows:
        triples = extract_triples_from_row(str(row), skip_substrings)
        if not triples:
            continue

        keys = [triple_key(s, p, o) for (s, p, o) in triples]
        gross += len(keys)

        for k in keys:
            trips_freq[k] = trips_freq.get(k, 0) + 1

        # co-occurrence within the same row
        uniq = list(dict.fromkeys(keys))  # stable unique
        for i, k in enumerate(uniq):
            if k not in cooc:
                cooc[k] = set()
            for j, other in enumerate(uniq):
                if i != j:
                    cooc[k].add(other)

    return trips_freq, cooc, gross


# ----------------------------
# Selection + expansion
# ----------------------------

@dataclass
class SelectionStats:
    gross_triples: int
    distinct_total: int
    target: int
    selected_core: int
    selected_expanded: int
    final_total: int


def select_triples_with_expansion(
    trips_freq: Dict[TripleKey, int],
    cooc: Dict[TripleKey, Set[TripleKey]],
    percent: float,
) -> Tuple[List[TripleKey], SelectionStats]:
    sorted_items = sorted(trips_freq.items(), key=lambda kv: kv[1], reverse=True)
    distinct_total = len(sorted_items)

    if distinct_total == 0:
        stats = SelectionStats(0, 0, 0, 0, 0, 0)
        return [], stats

    target = max(1, min(distinct_total, int(round(percent * distinct_total))))

    selected: List[TripleKey] = []
    used: Set[TripleKey] = set()
    core = 0
    expanded = 0

    for k, _freq in sorted_items:
        if k in used:
            continue
        used.add(k)
        selected.append(k)
        core += 1

        # expansion: add co-occurring triples until we hit target
        for other in cooc.get(k, set()):
            if other in used:
                continue
            used.add(other)
            selected.append(other)
            expanded += 1
            if len(selected) >= target:
                break

        if len(selected) >= target:
            break

    stats = SelectionStats(
        gross_triples=0,  # filled by caller
        distinct_total=distinct_total,
        target=target,
        selected_core=core,
        selected_expanded=expanded,
        final_total=len(selected),
    )
    return selected, stats


def write_nt(path: Path, keys: Sequence[TripleKey]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for k in keys:
            f.write(k + "\n")


def load_graph(nt_path: Path) -> Graph:
    g = Graph()
    # strict parse of a whole file is ok now (we’re writing valid nt)
    g.parse(nt_path, format="nt")
    return g


# ----------------------------
# Queries
# ----------------------------

def read_queries_lines(path: Path) -> List[str]:
    return path.read_text(encoding="utf-8").splitlines()


def build_select_query(line: str) -> Tuple[str, bool]:
    """
    Original queries file lines appear as comma-separated triple patterns.
    We keep that assumption.
    """
    parts = [p.strip() for p in line.split(",") if p.strip()]
    ex_flag = any(p.count("?") == 3 for p in parts)
    body = " ".join([p + " ." for p in parts])
    q = f"SELECT * WHERE {{ {body} }} LIMIT 1"
    return q, ex_flag


@dataclass
class EvalStats:
    total_test: int
    valid: int
    answered: int
    skipped: int
    t_local: float
    t_endpoint: float


def evaluate(
    g: Graph,
    queries_lines: List[str],
    test_indices: Set[int],
    endpoint_url: Optional[str],
    max_query_chars: int,
    max_query_vars: int,
) -> EvalStats:
    use_endpoint = bool(endpoint_url and SPARQLWrapper is not None)
    answered = valid = skipped = total = 0
    endpoint_time = 0.0

    t0 = time.time()
    for idx in test_indices:
        if idx < 0 or idx >= len(queries_lines):
            continue
        total += 1

        q, ex_flag = build_select_query(queries_lines[idx])
        if ex_flag or len(q) >= max_query_chars or q.count("?") >= max_query_vars:
            skipped += 1
            continue

        try:
            res = g.query(q)
            valid += 1
            if len(res) > 0:
                answered += 1
            else:
                if use_endpoint:
                    sparql = SPARQLWrapper(endpoint_url)
                    sparql.setReturnFormat(JSON)
                    sparql.setQuery(q)
                    tt = time.time()
                    try:
                        _ = sparql.queryAndConvert()
                    except Exception:
                        pass
                    endpoint_time += (time.time() - tt)
        except Exception:
            skipped += 1

    t_total = time.time() - t0
    return EvalStats(
        total_test=total,
        valid=valid,
        answered=answered,
        skipped=skipped,
        t_local=max(0.0, t_total - endpoint_time),
        t_endpoint=endpoint_time,
    )


# ----------------------------
# Train/test split (simple, reproducible)
# ----------------------------

def split_indices(n: int, test_fraction: float, seed: int) -> Tuple[Set[int], Set[int]]:
    """
    Simple reproducible split, avoids KFold complexity.
    """
    rng = pd.Series(range(n)).sample(frac=1.0, random_state=seed).tolist()
    cut = int(round(test_fraction * n))
    test = set(rng[:cut])
    train = set(rng[cut:])
    return train, test


# ----------------------------
# Metrics
# ----------------------------

def append_metrics(path: Path, percent: float, sel: SelectionStats, ev: EvalStats) -> None:
    coverage = (ev.answered / ev.valid) if ev.valid else 0.0
    valid_ratio = (ev.valid / ev.total_test) if ev.total_test else 0.0

    with path.open("a", encoding="utf-8") as f:
        f.write(f"\n{percent:.2f}\n")
        f.write(f"GROSS Triples {sel.gross_triples}\n")
        f.write(f"DISTINCT TOTAL TRIPLES {sel.distinct_total}\n")
        f.write(f"TARGET DISTINCT {sel.target}\n")
        f.write(f"CORE SELECTED {sel.selected_core} EXPANDED {sel.selected_expanded} FINAL {sel.final_total}\n")
        f.write(
            f"TOTAL TEST {ev.total_test} VALID {ev.valid} SKIPPED {ev.skipped} "
            f"PERCENT_VALID {valid_ratio:.4f}\n"
        )
        f.write(f"ANSWERED {ev.answered}\n")
        f.write(f"TIME_LOCAL {ev.t_local:.4f}\n")
        f.write(f"TIME_ENDPOINT {ev.t_endpoint:.4f}\n")
        f.write(f"COVERAGE {coverage:.4f}\n")


# ----------------------------
# Main
# ----------------------------

def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    cfg = parse_args()

    # Load summary as a single-column text file (like the original)
    summary_df = pd.read_fwf(cfg.summary_file, encoding="utf-8", colspecs=[(0, 500000)], names=["quer"])
    queries_lines = read_queries_lines(cfg.queries_file)

    train_idx, test_idx = split_indices(len(summary_df), cfg.test_fraction, cfg.fold_seed)
    train_rows = summary_df.iloc[list(train_idx)]["quer"].tolist()

    logging.info("Rows: total=%d train=%d test=%d", len(summary_df), len(train_idx), len(test_idx))
    logging.info("Endpoint: %s", cfg.endpoint_url or "(none)")

    for percent in cfg.percents:
        logging.info("=== percent=%.2f ===", percent)

        trips_freq, cooc, gross = build_counts_and_cooccurrence(train_rows, cfg.skip_substrings)
        keys, sel_stats = select_triples_with_expansion(trips_freq, cooc, percent)
        sel_stats = SelectionStats(
            gross_triples=gross,
            distinct_total=sel_stats.distinct_total,
            target=sel_stats.target,
            selected_core=sel_stats.selected_core,
            selected_expanded=sel_stats.selected_expanded,
            final_total=sel_stats.final_total,
        )

        out_nt = cfg.output_dir / f"summary_{int(percent*100):03d}.nt"
        write_nt(out_nt, keys)

        g = load_graph(out_nt)
        ev = evaluate(
            g=g,
            queries_lines=queries_lines,
            test_indices=test_idx,
            endpoint_url=cfg.endpoint_url,
            max_query_chars=cfg.max_query_chars,
            max_query_vars=cfg.max_query_vars,
        )

        coverage = (ev.answered / ev.valid) if ev.valid else 0.0
        logging.info(
            "distinct=%d target=%d final=%d | valid=%d answered=%d coverage=%.4f",
            sel_stats.distinct_total,
            sel_stats.target,
            sel_stats.final_total,
            ev.valid,
            ev.answered,
            coverage,
        )

        append_metrics(cfg.metrics_out, percent, sel_stats, ev)


if __name__ == "__main__":
    main()
