# lfs_modular_train_or_test.py
# Refactor of lfs_B_80.py into clean modular code.
# Keeps the same *idea* but removes hardcoded magic numbers and unsafe parsing.
# Source behavior derived from :contentReference[oaicite:1]{index=1}

from __future__ import annotations

import argparse
import logging
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple

import numpy as np
import pandas as pd
from rdflib import Graph
from sklearn.model_selection import KFold

try:
    from SPARQLWrapper import SPARQLWrapper, JSON  # optional
except Exception:
    SPARQLWrapper = None
    JSON = None


TripleKey = str  # "S P O ."


# ----------------------------
# Config
# ----------------------------

@dataclass(frozen=True)
class Config:
    summary_file: Path
    queries_file: Path
    output_nt: Path
    endpoint_url: Optional[str]
    percents: List[float]
    kfold_splits: int
    kfold_seed: int
    evaluate_on: str  # "train" or "test"
    max_query_chars: int
    max_query_vars: int
    skip_substrings: Tuple[str, ...]
    metrics_out: Path


def parse_args() -> Config:
    p = argparse.ArgumentParser(description="LFS summarization + query coverage evaluation (train/test selectable).")
    p.add_argument("summary_file", type=Path)
    p.add_argument("queries_file", type=Path)
    p.add_argument("output_nt", type=Path, help="Output N-Triples file written each percent (overwritten).")
    p.add_argument("--endpoint", type=str, default=None)
    p.add_argument("--percents", type=str, default="0.2,0.4,0.6,0.8,1.0")
    p.add_argument("--splits", type=int, default=5)
    p.add_argument("--seed", type=int, default=2)
    p.add_argument("--evaluate-on", choices=["train", "test"], default="train",
                   help="Match lfs_B_80.py with --evaluate-on train. (Earlier script used test.)")
    p.add_argument("--max-query-chars", type=int, default=700)
    p.add_argument("--max-query-vars", type=int, default=25)
    p.add_argument("--metrics-out", type=Path, default=Path("output.wiki10.txt"))

    args = p.parse_args()
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
        output_nt=args.output_nt,
        endpoint_url=args.endpoint,
        percents=percents,
        kfold_splits=args.splits,
        kfold_seed=args.seed,
        evaluate_on=args.evaluate_on,
        max_query_chars=args.max_query_chars,
        max_query_vars=args.max_query_vars,
        skip_substrings=skip_substrings,
        metrics_out=args.metrics_out,
    )


# ----------------------------
# RDF term extraction (robust)
# ----------------------------

_URI_RE = re.compile(r"^<https?://[^\s>]+>$")
_BNODE_RE = re.compile(r"^_:[A-Za-z][A-Za-z0-9]*$")
# permissive: "..." optionally @lang or ^^<datatype>
_LITERAL_RE = re.compile(r"^\".*\"(?:@[a-zA-Z]+(?:-[a-zA-Z0-9]+)*)?(?:\^\^<https?://[^\s>]+>)?$")

_NT_TOKEN_RE = re.compile(
    r"""
    <[^>]*>                  # URI
    | _:[A-Za-z][A-Za-z0-9]*  # blank node
    | "([^"\\]|\\.)*"         # literal with escapes
      (?:@[a-zA-Z]+(?:-[a-zA-Z0-9]+)*)?
      (?:\^\^<[^>]*>)?
    """,
    re.VERBOSE,
)


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


def extract_nt_terms(fragment: str) -> List[str]:
    return [m.group(0) for m in _NT_TOKEN_RE.finditer(fragment)]


def triple_key(s: str, p: str, o: str) -> TripleKey:
    return f"{s} {p} {o} ."


def extract_triples_from_summary_row(row: str, skip_substrings: Sequence[str]) -> List[TripleKey]:
    """
    Summary row contains multiple triple fragments separated by tabs.
    We parse each fragment for RDF terms and keep the first S,P,O.
    """
    if should_skip_line(row, skip_substrings):
        return []

    keys: List[TripleKey] = []
    for frag in str(row).split("\t"):
        frag = frag.strip()
        if not frag:
            continue
        terms = extract_nt_terms(frag)
        if len(terms) < 3:
            continue
        s, p, o = terms[0], terms[1], terms[2]
        if is_valid_nt(s, p, o):
            keys.append(triple_key(s, p, o))
    return keys


# ----------------------------
# Co-occurrence + frequency (simple row-based)
# ----------------------------

def build_counts_and_cooccurrence(
    train_rows: Iterable[str],
    skip_substrings: Sequence[str],
) -> Tuple[Dict[TripleKey, int], Dict[TripleKey, Set[TripleKey]], int]:
    trips_freq: Dict[TripleKey, int] = {}
    cooc: Dict[TripleKey, Set[TripleKey]] = {}
    gross = 0

    for row in train_rows:
        keys = extract_triples_from_summary_row(row, skip_substrings)
        if not keys:
            continue

        gross += len(keys)
        for k in keys:
            trips_freq[k] = trips_freq.get(k, 0) + 1

        uniq = list(dict.fromkeys(keys))
        for i, k in enumerate(uniq):
            cooc.setdefault(k, set())
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


def select_triples(trips_freq: Dict[TripleKey, int], cooc: Dict[TripleKey, Set[TripleKey]], percent: float
                  ) -> Tuple[List[TripleKey], SelectionStats]:
    sorted_items = sorted(trips_freq.items(), key=lambda kv: kv[1], reverse=True)
    distinct_total = len(sorted_items)
    if distinct_total == 0:
        return [], SelectionStats(0, 0, 0, 0, 0, 0)

    target = max(1, min(distinct_total, int(round(percent * distinct_total))))
    selected: List[TripleKey] = []
    used: Set[TripleKey] = set()
    core = expanded = 0

    for k, _freq in sorted_items:
        if k in used:
            continue
        used.add(k)
        selected.append(k)
        core += 1

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

    return selected, SelectionStats(
        gross_triples=0,  # fill by caller
        distinct_total=distinct_total,
        target=target,
        selected_core=core,
        selected_expanded=expanded,
        final_total=len(selected),
    )


def write_nt(path: Path, keys: Sequence[TripleKey]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for k in keys:
            f.write(k + "\n")


def load_graph(nt_path: Path) -> Graph:
    g = Graph()
    g.parse(nt_path, format="nt")
    return g


# ----------------------------
# Query evaluation
# ----------------------------

def read_queries_lines(path: Path) -> List[str]:
    return path.read_text(encoding="utf-8").splitlines()


def build_select_query(line: str) -> Tuple[str, bool]:
    parts = [p.strip() for p in line.split(",") if p.strip()]
    ex_flag = any(p.count("?") == 3 for p in parts)  # matches original heuristic
    body = " ".join([p + " ." for p in parts])
    q = f"SELECT * WHERE {{ {body} }} LIMIT 1"
    return q, ex_flag


@dataclass
class EvalStats:
    total_considered: int
    valid: int
    answered: int
    skipped: int
    t_local: float
    t_endpoint: float


def evaluate_queries(
    g: Graph,
    queries_lines: List[str],
    indices_to_use: Set[int],
    endpoint_url: Optional[str],
    max_query_chars: int,
    max_query_vars: int,
) -> EvalStats:
    use_endpoint = bool(endpoint_url and SPARQLWrapper is not None)

    total = valid = answered = skipped = 0
    endpoint_time = 0.0
    t0 = time.time()

    for idx in sorted(indices_to_use):
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
                    t1 = time.time()
                    try:
                        _ = sparql.queryAndConvert()
                    except Exception:
                        pass
                    endpoint_time += (time.time() - t1)
        except Exception:
            skipped += 1

    t_total = time.time() - t0
    return EvalStats(
        total_considered=total,
        valid=valid,
        answered=answered,
        skipped=skipped,
        t_local=max(0.0, t_total - endpoint_time),
        t_endpoint=endpoint_time,
    )


# ----------------------------
# Metrics
# ----------------------------

def append_metrics(metrics_path: Path, percent: float, sel: SelectionStats, ev: EvalStats) -> None:
    coverage = (ev.answered / ev.valid) if ev.valid else 0.0
    with metrics_path.open("a", encoding="utf-8") as f:
        f.write(f"\n{percent:.2f}\n")
        f.write(f"GROSS Triples {sel.gross_triples}\n")
        f.write(f"DISTINCT TOTAL TRIPLES {sel.distinct_total}\n")
        f.write(f"TARGET DISTINCT {sel.target}\n")
        f.write(f"CORE {sel.selected_core} EXPANDED {sel.selected_expanded} FINAL {sel.final_total}\n")
        f.write(f"TOTAL {ev.total_considered} VALID {ev.valid} SKIPPED {ev.skipped}\n")
        f.write(f"ANSWERED {ev.answered}\n")
        f.write(f"TIME_LFS {ev.t_local:.4f}\n")
        f.write(f"TIME_ENDPOINT {ev.t_endpoint:.4f}\n")
        f.write(f"COVERAGE {coverage:.4f}\n")


# ----------------------------
# Main
# ----------------------------

def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    cfg = parse_args()

    # Read summary & queries
    summary_df = pd.read_fwf(cfg.summary_file, encoding="utf-8", colspecs=[(0, 500000)], names=["quer"])
    queries_lines = read_queries_lines(cfg.queries_file)

    # KFold split (matches original)
    kf = KFold(n_splits=cfg.kfold_splits, shuffle=True, random_state=cfg.kfold_seed)
    train_idx, test_idx = next(iter(kf.split(summary_df)))

    train_rows = summary_df.iloc[train_idx]["quer"].tolist()
    train_indices = set(summary_df.iloc[train_idx].index.tolist())
    test_indices = set(summary_df.iloc[test_idx].index.tolist())

    indices_for_eval = train_indices if cfg.evaluate_on == "train" else test_indices
    logging.info("evaluate_on=%s | train=%d test=%d", cfg.evaluate_on, len(train_indices), len(test_indices))
    logging.info("endpoint=%s", cfg.endpoint_url or "(none)")

    # overwrite metrics file like original did (open with "w")
    cfg.metrics_out.write_text("", encoding="utf-8")

    for percent in cfg.percents:
        logging.info("=== percent=%.2f ===", percent)

        trips_freq, cooc, gross = build_counts_and_cooccurrence(train_rows, cfg.skip_substrings)
        keys, sel = select_triples(trips_freq, cooc, percent)
        sel = SelectionStats(
            gross_triples=gross,
            distinct_total=sel.distinct_total,
            target=sel.target,
            selected_core=sel.selected_core,
            selected_expanded=sel.selected_expanded,
            final_total=sel.final_total,
        )

        write_nt(cfg.output_nt, keys)
        g = load_graph(cfg.output_nt)

        ev = evaluate_queries(
            g=g,
            queries_lines=queries_lines,
            indices_to_use=indices_for_eval,
            endpoint_url=cfg.endpoint_url,
            max_query_chars=cfg.max_query_chars,
            max_query_vars=cfg.max_query_vars,
        )

        coverage = (ev.answered / ev.valid) if ev.valid else 0.0
        logging.info("distinct=%d target=%d final=%d | valid=%d answered=%d coverage=%.4f",
                     sel.distinct_total, sel.target, sel.final_total, ev.valid, ev.answered, coverage)

        append_metrics(cfg.metrics_out, percent, sel, ev)


if __name__ == "__main__":
    main()
