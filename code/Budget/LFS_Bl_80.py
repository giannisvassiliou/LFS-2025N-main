# lfs_bl_80_modular_v2.py
# Robust-parser modular rewrite of lfs_Bl_80.py
# Derived from :contentReference[oaicite:1]{index=1}

from __future__ import annotations

import argparse
import logging
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Set, Tuple

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

    dbpedia5_file: Path
    percents: List[float]

    kfold_splits: int
    kfold_seed: int

    distinct_total_hardcoded: int  # lfs_Bl_80.py uses tt=37804

    max_query_chars: int
    max_query_vars: int

    query_limit: int              # lfs_Bl_80: LIMIT 5 for local
    min_answers_to_count: int     # count answered if >=5 rows

    skip_substrings: Tuple[str, ...]
    metrics_out: Path


def parse_args() -> Config:
    p = argparse.ArgumentParser(description="Modular refactor of lfs_Bl_80.py with robust RDF term parsing.")
    p.add_argument("summary_file", type=Path)
    p.add_argument("queries_file", type=Path)
    p.add_argument("output_nt", type=Path)
    p.add_argument("percent", type=float, help="Kept for CLI compatibility with original; not used (script loops percents).")
    p.add_argument("--endpoint", type=str, default=None)

    p.add_argument("--dbpedia5", type=Path, default=Path("5dbpedia_0_1000.txt"))
    p.add_argument("--percents", type=str, default="0.2,0.4,0.6,0.8,1.0")

    p.add_argument("--splits", type=int, default=5)
    p.add_argument("--seed", type=int, default=2)

    p.add_argument("--distinct-total", type=int, default=37804)

    p.add_argument("--max-query-chars", type=int, default=700)
    p.add_argument("--max-query-vars", type=int, default=25)

    p.add_argument("--query-limit", type=int, default=5)
    p.add_argument("--min-answers", type=int, default=5)

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
        dbpedia5_file=args.dbpedia5,
        percents=percents,
        kfold_splits=args.splits,
        kfold_seed=args.seed,
        distinct_total_hardcoded=args.distinct_total,
        max_query_chars=args.max_query_chars,
        max_query_vars=args.max_query_vars,
        query_limit=args.query_limit,
        min_answers_to_count=args.min_answers,
        skip_substrings=skip_substrings,
        metrics_out=args.metrics_out,
    )


# ----------------------------
# Robust RDF term tokenizer
# ----------------------------

_NT_TOKEN_RE = re.compile(
    r"""
    <[^>]*>                   # URI
    | _:[A-Za-z][A-Za-z0-9]*   # blank node
    | "([^"\\]|\\.)*"          # literal with escapes
      (?:@[a-zA-Z]+(?:-[a-zA-Z0-9]+)*)?   # optional lang
      (?:\^\^<[^>]*>)?                    # optional datatype
    """,
    re.VERBOSE,
)

_URI_RE = re.compile(r"^<https?://[^\s>]+>$")
_BNODE_RE = re.compile(r"^_:[A-Za-z][A-Za-z0-9]*$")
_LITERAL_RE = re.compile(r"^\".*\"(?:@[a-zA-Z]+(?:-[a-zA-Z0-9]+)*)?(?:\^\^<[^>]+>)?$")


def is_valid_nt(s: str, p: str, o: str) -> bool:
    if not (_URI_RE.match(s) or _BNODE_RE.match(s)):
        return False
    if not _URI_RE.match(p):
        return False
    if not (_URI_RE.match(o) or _BNODE_RE.match(o) or _LITERAL_RE.match(o)):
        return False
    return True


def extract_terms(text: str) -> List[str]:
    return [m.group(0) for m in _NT_TOKEN_RE.finditer(text)]


def triple_key(s: str, p: str, o: str) -> TripleKey:
    return f"{s} {p} {o} ."


def should_skip_line(line: str, skip_substrings: Sequence[str]) -> bool:
    return any(s in line for s in skip_substrings)


def parse_summary_row(row_text: str, skip_substrings: Sequence[str]) -> List[TripleKey]:
    """
    lfs_Bl_80 expects tab-separated fragments.
    This robust parser extracts S,P,O using RDF term regex (supports literals with spaces).
    """
    if should_skip_line(row_text, skip_substrings):
        return []

    keys: List[TripleKey] = []
    for frag in str(row_text).split("\t"):
        frag = frag.strip()
        if not frag:
            continue

        # mimic original stripping of weird quotes/asterisks
        frag = frag.replace("*", "").replace("“", "").replace("„", "").replace("”", "")

        terms = extract_terms(frag)
        if len(terms) < 3:
            continue
        s, p, o = terms[0], terms[1], terms[2]

        # original behavior: if object looks like <...> but not http, convert to literal
        if o.startswith("<") and "http" not in o:
            o = f"\"{o[1:-1]}\""

        if is_valid_nt(s, p, o):
            keys.append(triple_key(s, p, o))

    return keys


# ----------------------------
# Helpers (grouping like original)
# ----------------------------

def split_list(lst: List[str], n: int) -> List[List[str]]:
    k, m = divmod(len(lst), n)
    return [lst[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n)]


# ----------------------------
# Build frequency, indextrip, tripspre (comma grouping from query lines)
# ----------------------------

def build_structures(
    train_df: pd.DataFrame,
    qlines: List[str],
    skip_substrings: Sequence[str],
) -> Tuple[Dict[TripleKey, int], Dict[TripleKey, Set[TripleKey]], Dict[TripleKey, Set[int]], int]:
    trips: Dict[TripleKey, int] = {}
    tripspre: Dict[TripleKey, Set[TripleKey]] = {}
    indextrip: Dict[TripleKey, Set[int]] = {}
    gross = 0

    for idx, row in train_df.iterrows():
        keys = parse_summary_row(str(row["quer"]), skip_substrings)
        if not keys:
            continue

        gross += len(keys)

        for k in keys:
            trips[k] = trips.get(k, 0) + 1
            indextrip.setdefault(k, set()).add(int(idx))

        # comma-based grouping derived from query line at the same index (like original)
        if 0 <= idx < len(qlines):
            groups_n = qlines[idx].count(",") + 1
        else:
            groups_n = 1

        groups = split_list(keys, groups_n) if groups_n > 1 else [[k] for k in keys]
        for group in groups:
            for t in group:
                tripspre.setdefault(t, set())
                for other in group:
                    if other != t:
                        tripspre[t].add(other)

    return trips, tripspre, indextrip, gross


# ----------------------------
# Plus5 DBpedia expansion (robust)
# ----------------------------

def fix_triple_dbpedia(fragment: str) -> Optional[TripleKey]:
    """
    Robust equivalent of fix_triple() in lfs_Bl_80:
    - expects at least 3 RDF terms (S,P,O)
    - if object is <...> without http, convert to literal
    """
    terms = extract_terms(fragment)
    if len(terms) < 3:
        return None
    s, p, o = terms[0], terms[1], terms[2]
    if o.startswith("<") and "http" not in o:
        o = f"\"{o[1:-1]}\""
    if not is_valid_nt(s, p, o):
        return None
    return triple_key(s, p, o)


# ----------------------------
# Output formatting and graph loading (kept compatible with original)
# ----------------------------

def write_line_like_original(k: TripleKey) -> str:
    parts = k.split()
    if len(parts) < 4:
        return ""
    s, p, o = parts[0], parts[1], parts[2]
    # original wrote: s p  o \t.
    return f"{s} {p} {o} \t."


def write_summary(
    sorted_trips: List[Tuple[TripleKey, int]],
    indextrip: Dict[TripleKey, Set[int]],
    df5: pd.DataFrame,
    trips_freq: Dict[TripleKey, int],
    output_path: Path,
    percent: float,
    distinct_total_hardcoded: int,
) -> Tuple[int, int, int, int]:
    """
    Returns (nodis, noexp, noplus5, tsf2)
    Note: tripspre expansion is commented out in your lfs_Bl_80; we keep it off too.
    """
    notr = int(percent * distinct_total_hardcoded)

    used: Set[TripleKey] = set()
    tripl: Set[TripleKey] = set()

    nodis = 0
    noexp = 0
    noplus5 = 0
    tsf2 = 0

    out_lines: List[str] = []

    for k, _freq in sorted_trips:
        if k in used or k in tripl:
            continue

        # validate
        parts = k.split()
        if len(parts) < 4:
            continue
        s, p, o = parts[0], parts[1], parts[2]
        if not is_valid_nt(s, p, o):
            continue

        nodis += 1
        tsf2 += trips_freq.get(k, 0)
        out_lines.append(write_line_like_original(k))
        used.add(k)

        # plus5: for each index where k appears, read df5 row and add any triples that exist in trips_freq
        for ia in indextrip.get(k, set()):
            if ia < 0 or ia >= len(df5):
                continue
            row5 = str(df5["quer"][ia])
            for frag in row5.split("\t"):
                frag = frag.strip()
                if not frag:
                    continue

                kk = fix_triple_dbpedia(frag)
                if not kk:
                    continue
                if kk in used or kk in tripl:
                    continue

                # original only adds if it exists in trips_freq / sorted_trips dict
                if kk in trips_freq:
                    out_lines.append(write_line_like_original(kk))
                    tsf2 += trips_freq.get(kk, 0)
                    noplus5 += 1
                    tripl.add(kk)
                    used.add(kk)

        if (nodis + noexp + noplus5) >= notr:
            break

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as f:
        for line in out_lines:
            if line:
                f.write(line + "\n")

    return nodis, noexp, noplus5, tsf2


def load_graph_line_by_line(nt_path: Path) -> Graph:
    g = Graph()
    with nt_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            try:
                mini = Graph()
                mini.parse(data=line, format="nt11")
                for s, p, o in mini:
                    g.add((s, p, o))
            except Exception:
                continue
    return g


# ----------------------------
# Query evaluation (trainlist, LIMIT 5, answered if >=5 results)
# ----------------------------

def build_select_query(query_line: str, limit: int) -> Tuple[str, bool]:
    parts = [p.strip() for p in query_line.split(",") if p.strip()]
    ex_flag = any(p.count("?") == 3 for p in parts)
    body = "".join([p + " ." for p in parts])
    q = f"SELECT * WHERE {{ {body} }} LIMIT {limit}"
    return q, ex_flag


@dataclass
class EvalStats:
    total: int
    valid: int
    answered: int
    elapsed_local: float
    elapsed_endpoint: float


def evaluate_queries(
    g: Graph,
    df_queries: pd.DataFrame,
    indices_to_use: Set[int],  # lfs_Bl_80 uses trainlist
    endpoint_url: Optional[str],
    max_query_chars: int,
    max_query_vars: int,
    query_limit: int,
    min_answers_to_count: int,
) -> EvalStats:
    use_endpoint = bool(endpoint_url and SPARQLWrapper is not None)

    total = valid = answered = 0
    endpoint_time = 0.0
    t0 = time.time()

    for idx, row in df_queries.iterrows():
        if idx not in indices_to_use:
            continue

        total += 1
        q, ex_flag = build_select_query(str(row["quer"]), limit=query_limit)
        if ex_flag or len(q) >= max_query_chars or q.count("?") >= max_query_vars:
            continue

        try:
            res = g.query(q)
            valid += 1
            if len(res) >= min_answers_to_count:
                answered += 1
            else:
                if use_endpoint and len(res) == 0:
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
            continue

    elapsed = time.time() - t0
    return EvalStats(
        total=total,
        valid=valid,
        answered=answered,
        elapsed_local=max(0.0, elapsed - endpoint_time),
        elapsed_endpoint=endpoint_time,
    )


def append_metrics(
    metrics_path: Path,
    percent: float,
    gross: int,
    nodis: int,
    noexp: int,
    noplus5: int,
    tsf2: int,
    ev: EvalStats,
) -> None:
    coverage = (ev.answered / ev.valid) if ev.valid else 0.0
    with metrics_path.open("a", encoding="utf-8") as f:
        f.write(f"\n{percent:.2f}\n")
        f.write(f"GROSS Triples {gross}\n")
        f.write(f"TRIPLES IN SELECTED PORTION plus5 {noplus5} EXPA {noexp} DISTSEL {nodis} athr {nodis+noexp+noplus5} tsf2 {tsf2}\n")
        f.write(f"TOTAL QUERIES {ev.total} TOTAL VALID {ev.valid}\n")
        f.write(f"ANSWERED {ev.answered}\n")
        f.write(f"Elapsed time for LFS {ev.elapsed_local}\n")
        f.write(f"Elapsed time for ENDPOINT {ev.elapsed_endpoint}\n")
        f.write(f"COVERAGE FOR LFS {coverage}\n")


# ----------------------------
# Main
# ----------------------------

def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    cfg = parse_args()

    logging.info("Summary:  %s", cfg.summary_file)
    logging.info("Queries:  %s", cfg.queries_file)
    logging.info("DBpedia5: %s", cfg.dbpedia5_file)
    logging.info("Output:   %s", cfg.output_nt)
    logging.info("Endpoint: %s", cfg.endpoint_url or "(none)")

    # Clear metrics file like original (opened with "w")
    cfg.metrics_out.write_text("", encoding="utf-8")

    # Read input files similarly (wide single col)
    df_summary = pd.read_fwf(cfg.summary_file, encoding="utf-8", colspecs=[(0, 200000)], names=["quer"])
    df_queries = pd.read_fwf(cfg.queries_file, encoding="utf-8", colspecs=[(0, 4500)], names=["quer"])
    qlines = cfg.queries_file.read_text(encoding="utf-8").splitlines(True)
    df5 = pd.read_fwf(cfg.dbpedia5_file, encoding="utf-8", colspecs=[(0, 200000)], names=["quer"])

    # KFold split (first fold only)
    kf = KFold(n_splits=cfg.kfold_splits, shuffle=True, random_state=cfg.kfold_seed)
    train_idx, test_idx = next(iter(kf.split(df_summary)))

    train = df_summary.iloc[train_idx]
    train_indices = set(train.index.tolist())  # lfs_Bl_80 evaluates queries on trainlist

    for percent in cfg.percents:
        logging.info("=== percent=%.2f ===", percent)

        trips, tripspre, indextrip, gross = build_structures(train, qlines, cfg.skip_substrings)
        sorted_trips = sorted(trips.items(), key=lambda kv: kv[1], reverse=True)

        nodis, noexp, noplus5, tsf2 = write_summary(
            sorted_trips=sorted_trips,
            indextrip=indextrip,
            df5=df5,
            trips_freq=trips,
            output_path=cfg.output_nt,
            percent=percent,
            distinct_total_hardcoded=cfg.distinct_total_hardcoded,
        )

        g = load_graph_line_by_line(cfg.output_nt)
        ev = evaluate_queries(
            g=g,
            df_queries=df_queries,
            indices_to_use=train_indices,
            endpoint_url=cfg.endpoint_url,
            max_query_chars=cfg.max_query_chars,
            max_query_vars=cfg.max_query_vars,
            query_limit=cfg.query_limit,
            min_answers_to_count=cfg.min_answers_to_count,
        )

        logging.info(
            "valid=%d answered=%d coverage=%.4f",
            ev.valid,
            ev.answered,
            (ev.answered / ev.valid) if ev.valid else 0.0,
        )
        append_metrics(cfg.metrics_out, percent, gross, nodis, noexp, noplus5, tsf2, ev)


if __name__ == "__main__":
    main()
