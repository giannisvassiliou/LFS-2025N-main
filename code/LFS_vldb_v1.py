#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
LFS experiment script – behavior-aligned with original version,
with:
  - explicit endpoint timing
  - explicit local query timing
  - optional --novel-only evaluation.

Pipeline:
1. Read a file with "queries" (summary_file) – each line may contain one or more triples.
2. Read a file with query patterns (queries_file).
3. Split summary_file into train / test using K-Fold.
4. From train, extract triples (with the SAME cleaning as original script) and count frequencies.
5. Select top-N% triples (by frequency) and write them to an output file in (almost) the same
   line format as the original summary (extra spaces + tab + '.') so rdflib parsing behaves similarly.
6. Load the summary output into an rdflib Graph.
7. Evaluate test queries from queries_file against this graph (and optionally a SPARQL endpoint).
"""

import argparse
import re
import time
from typing import Dict, List, Tuple, Optional

import numpy as np
import pandas as pd
from rdflib import Graph
from SPARQLWrapper import SPARQLWrapper, JSON
from sklearn.model_selection import KFold


# ---------------------------------------------------------------------------
# Command-line parsing
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build an LFS-style summary from RDF-like triples and evaluate queries."
    )
    parser.add_argument(
        "summary_file",
        help="Input file with triples (one line per 'query' or group of triples).",
    )
    parser.add_argument(
        "queries_file",
        help="Input file with query patterns.",
    )
    parser.add_argument(
        "summary_output",
        help="Output file for the summary (will be parsed as N-Triples-ish).",
    )
    parser.add_argument(
        "percent",
        type=float,
        help="Fraction of distinct triples to select into the summary (e.g., 1.0 for 100%%, 0.1 for 10%%).",
    )
    parser.add_argument(
        "--endpoint",
        help="Optional SPARQL endpoint URL to query when the local summary does not answer.",
        default=None,
    )
    parser.add_argument(
        "--novel-only",
        action="store_true",
        help="If set, evaluate coverage only on test queries not present in the train set.",
    )
    return parser.parse_args()


# ---------------------------------------------------------------------------
# Original-style helpers
# ---------------------------------------------------------------------------

def is_valid_nt_term(subject: str, predicate: str, obj: str) -> bool:
    """
    Same logic as original is_valid(subject, predicate, obj).

    - subject: URI or blank node
    - predicate: URI
    - object: URI, blank node, or literal
    """
    uri_pattern = r"^<https?://[^\s>]+>$"              # <http://example.org/...>
    literal_pattern = r"^\".*\"(?:\^\^<https?://[^\s>]+>)?$"  # "text" or "text"^^<datatype>
    blank_node_pattern = r"^_:([A-Za-z][A-Za-z0-9]*)$" # _:bnode

    # Subject must be URI or blank node
    if not (re.match(uri_pattern, subject) or re.match(blank_node_pattern, subject)):
        return False

    # Predicate must be URI
    if not re.match(uri_pattern, predicate):
        return False

    # Object: URI, blank node, or literal
    if not (re.match(uri_pattern, obj) or
            re.match(blank_node_pattern, obj) or
            re.match(literal_pattern, obj)):
        return False

    return True


def clean_tokens_like_original(sub: str, pre: str, obj: str) -> Tuple[str, str, str]:
    """
    Apply the same (quirky) cleaning rules as in the original script
    BEFORE building the triple string.
    """
    # Remove weird characters
    for ch in ["*", "“", "„", "“"]:
        sub = sub.replace(ch, "")
        pre = pre.replace(ch, "")
        obj = obj.replace(ch, "")

    # Handle quotes in subject
    if '"' in sub:
        sub = sub.replace('"', "")
        sub = "_:" + sub  # turn into blank node

    # If looks like <...> but not http, strip quotes
    if "http" not in sub and sub and sub[0] == "<":
        sub = sub.replace('"', "")

    if "http" not in pre and pre and pre[0] == "<":
        pre = pre.replace('"', "")

    if "http" not in obj and obj and obj[0] == "<":
        obj = obj.replace('"', "")

    # Turn non-http <...> predicate and object into quoted literals
    if "http" not in pre and pre and pre[0] == "<":
        pre = pre.replace('"', "").replace("<", "").replace(">", "")
        pre = '"' + pre + '"'

    if "http" not in obj and obj and obj[0] == "<":
        obj = obj.replace('"', "").replace("<", "").replace(">", "")
        obj = '"' + obj + '"'

    return sub, pre, obj


# ---------------------------------------------------------------------------
# Data loading & splitting
# ---------------------------------------------------------------------------

def read_data(summary_file: str, queries_file: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Read the summary and query files as single-column DataFrames with column 'quer'.
    """
    df_summary = pd.read_fwf(
        summary_file,
        encoding="utf-8",
        colspecs=[(0, 344500)],
        names=["quer"],
    )
    df_queries = pd.read_fwf(
        queries_file,
        encoding="utf-8",
        colspecs=[(0, 55500)],
        names=["quer"],
    )
    return df_summary, df_queries


def kfold_split(df: pd.DataFrame, n_splits: int = 5, random_state: int = 42) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Perform a single K-Fold split and return (train_df, test_df).

    We use only the first fold, same as original behavior.
    """
    kf = KFold(n_splits=n_splits, shuffle=True, random_state=random_state)
    splits = list(kf.split(df))
    train_idx, test_idx = splits[0]
    train = df.iloc[train_idx]
    test = df.iloc[test_idx]
    print(f"Train size: {len(train)}")
    print(f"Test size:  {len(test)}")
    return train, test


# ---------------------------------------------------------------------------
# Triple extraction & selection (aligned with original)
# ---------------------------------------------------------------------------

def extract_triple_counts(train_df: pd.DataFrame) -> Dict[str, int]:
    """
    Extract triples from train_df using the same logic as the original script,
    and count frequencies.

    The key in the returned dict is the EXACT triple string:
        'sub pre obj .'
    built AFTER cleaning.
    """
    triple_counts: Dict[str, int] = {}
    total_triples = 0

    print("Extracting triples from training data...")

    for _, row in train_df.iterrows():
        line = str(row["quer"])

        # Same "bad pattern" skip logic as original
        if any(bad in line for bad in [
            "2023-01-31T01:09:43Z",
            "<<",
            "math",
            "<application/x-httpd-php>",
            "10766787-n",
            "1848831457",
        ]):
            continue

        fragments = line.split("\t")
        for fragment in fragments:
            fragment = fragment.strip()
            if not fragment:
                continue

            tokens = fragment.split()
            if len(tokens) < 3:
                continue

            sub, pre, obj = tokens[0], tokens[1], tokens[2]

            try:
                # Clean tokens as in original
                sub, pre, obj = clean_tokens_like_original(sub, pre, obj)

                # Same extra checks (resa, ress2) as original
                resa = f"{sub} {pre} {obj} . \n"
                ress2 = f"{sub} {pre} {obj} . \n"

                if (resa == '" .\n' or
                        ress2 == " > . \n" or
                        "<<" in ress2 or
                        "math" in resa or
                        len(ress2) <= 10):
                    continue

                triple_str = f"{sub} {pre} {obj} ."
                total_triples += 1
                triple_counts[triple_str] = triple_counts.get(triple_str, 0) + 1

            except Exception:
                # Ignore malformed fragments (as original did)
                continue

    print(f"Total triples seen in train: {total_triples}")
    print(f"Distinct triples: {len(triple_counts)}")
    return triple_counts


def select_top_triples(
    triple_counts: Dict[str, int],
    percent: float,
    output_path: str,
) -> Tuple[int, int]:
    """
    Select the top-N% most frequent triples and write them in a line format
    as close as possible to the original:

        sub pre  obj \t.

    Note:
        - We still use percent as a fraction (0 < percent <= 1).
        - We apply is_valid_nt_term() on the tokens, as in original.
    """
    if percent <= 0:
        raise ValueError("percent must be > 0 (use e.g. 0.1 for 10%, 1.0 for 100%).")
    if percent > 1:
        percent = 1.0

    # Sort by descending frequency
    sorted_triples = sorted(triple_counts.items(), key=lambda kv: kv[1], reverse=True)
    total_distinct = len(sorted_triples)
    to_select = max(1, int(round(percent * total_distinct)))

    print(
        f"Selecting top {to_select} triples ({percent * 100:.2f}% of {total_distinct} distinct triples)."
    )

    distinct_selected = 0
    total_written = 0

    with open(output_path, "w", encoding="utf-8") as f_out:
        for triple_str, _freq in sorted_triples:
            tokens = triple_str.split()
            if len(tokens) < 4:
                continue

            sub, pre, obj = tokens[0], tokens[1], tokens[2]

            # validation as in original (after cleaning)
            if not is_valid_nt_term(sub, pre, obj):
                continue

            # small cleanup like original
            sub = sub.replace("/n", "")
            pre = pre.replace("/n", "")
            obj = obj.replace("/n", "")

            lit = obj
            # reproduce the odd formatting: extra space before object, tab before '.'
            line = f"{sub} {pre}  {lit} \t."
            line = line.replace("^", "")

            f_out.write(line + "\n")
            distinct_selected += 1
            total_written += 1

            if distinct_selected >= to_select:
                break

    print(f"Distinct triples written: {distinct_selected}")
    print(f"Total lines written:      {total_written}")
    return distinct_selected, total_written


# ---------------------------------------------------------------------------
# Graph loading
# ---------------------------------------------------------------------------

def load_graph_from_nt(path: str) -> Graph:
    """
    Load the summary file into an rdflib Graph, line by line with 'nt11' format.
    This is robust to minor spacing / tab weirdness.
    """
    print(f"Loading summary graph from {path} ...")
    g = Graph()

    with open(path, "r", encoding="utf-8") as f:
        for i, line in enumerate(f, start=1):
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            try:
                mini = Graph()
                mini.parse(data=line, format="nt11")
                for s, p, o in mini:
                    g.add((s, p, o))
            except Exception as e:
                # Same spirit as original: skip bad lines
                print(f"[WARN] Could not parse line {i}: {e}")

    print(f"Summary graph loaded with {len(g)} triples.")
    return g


# ---------------------------------------------------------------------------
# Query building & evaluation (aligned with original logic)
# ---------------------------------------------------------------------------

def build_sparql_query_from_line(line: str) -> Optional[str]:
    """
    Convert a queries_file line into a SPARQL SELECT.

    Same logic as original:
        - split by commas
        - append ' .' to each part
        - reject if any fragment has exactly 3 '?' or if query is too long or has too many vars.
    """
    line = str(line)
    parts = line.split(",")

    where_fragments: List[str] = []
    too_complex = False

    for part in parts:
        part = part.strip()
        if not part:
            continue
        if part.count("?") == 3:
            too_complex = True
        where_fragments.append(part + " .")

    if not where_fragments or too_complex:
        return None

    where_clause = " ".join(where_fragments)
    query = f"SELECT * WHERE {{ {where_clause} }} LIMIT 1"

    if len(query) >= 700 or query.count("?") >= 25:
        return None

    return query


def evaluate_queries(
    g: Graph,
    df_queries: pd.DataFrame,
    test_indices: List[int],
    endpoint_url: Optional[str] = None,
) -> None:
    """
    Evaluate queries with indices in test_indices against the local graph (and optionally endpoint),
    and print stats similar to the original script.

    Also measures:
      - total_time
      - endpoint_time (real: sum of endpoint.queryAndConvert() times)
      - local_query_time (sum of g.query() times)
      - overhead_time = total_time - local_query_time - endpoint_time
    """
    total_test = 0      # synolikos
    valid_queries = 0   # noall
    answered_local = 0  # ansq

    endpoint_time = 0.0
    local_query_time = 0.0

    start_time = time.time()

    endpoint = None
    if endpoint_url:
        print(f"Using remote endpoint: {endpoint_url}")
        endpoint = SPARQLWrapper(endpoint_url)
        endpoint.setReturnFormat(JSON)

    for idx, row in df_queries.iterrows():
        if idx not in test_indices:
            continue

        total_test += 1

        sparql_query = build_sparql_query_from_line(row["quer"])
        if sparql_query is None:
            # invalid or too complex query -> behaves like original (noall--)
            continue

        valid_queries += 1

        # Local query timing
        try:
            t0_local = time.time()
            res = g.query(sparql_query)
            local_query_time += time.time() - t0_local
        except Exception as e:
            print(f"[WARN] Local query error at index {idx}: {e}")
            valid_queries -= 1
            continue

        if len(res) > 0:
            answered_local += 1
        else:
            # if endpoint provided, try it (this only affects timing, not local coverage)
            if endpoint is not None:
                try:
                    t0_ep = time.time()
                    endpoint.setQuery(sparql_query)
                    _remote_res = endpoint.queryAndConvert()
                    endpoint_time += time.time() - t0_ep
                except Exception as e:
                    print(f"[WARN] Endpoint query error at index {idx}: {e}")

    total_time = time.time() - start_time
    overhead_time = total_time - local_query_time - endpoint_time
    local_total_time = total_time - endpoint_time  # matching "LFS time" in original

    print("\n================= EVALUATION =================")
    print(f"Total test queries:           {total_test}")
    print(f"Valid test queries:           {valid_queries}")
    print(f"Answered by local LFS:        {answered_local}")
    if valid_queries > 0:
        coverage = answered_local / valid_queries
        print(f"Coverage (local only):        {coverage:.4f}")
    else:
        print("Coverage (local only):        N/A (no valid queries).")

    print(f"\nElapsed time (total):         {total_time:.4f} s")
    print(f"Elapsed time (local queries): {local_query_time:.4f} s")
    print(f"Elapsed time (endpoint):      {endpoint_time:.4f} s")
    print(f"Elapsed time (overhead):      {overhead_time:.4f} s")
    print(f"Elapsed time (local total):   {local_total_time:.4f} s  # total - endpoint")
    print("=============================================\n")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    np.random.seed(42)

    args = parse_args()
    print(f"Summary file: {args.summary_file}")
    print(f"Queries file: {args.queries_file}")
    print(f"Percent:      {args.percent}")
    print(f"Novel only?:  {args.novel_only}")
    if args.endpoint:
        print("Endpoint:     will be used")
    else:
        print("Endpoint:     none")

    # 1) Read data
    df_summary, df_queries = read_data(args.summary_file, args.queries_file)

    # 2) Train/test split
    print("\nSplitting the dataset into train/test...")
    train_df, test_df = kfold_split(df_summary)

    # 2b) Pick indices: full test set or novel-only
    if args.novel_only:
        ns = test_df[~test_df["quer"].isin(train_df["quer"])]
        test_indices = list(ns.index)
        print(f"Novel test queries: {len(test_indices)} out of {len(test_df)} total test queries")
    else:
        test_indices = list(test_df.index)
        print(f"Using all test queries: {len(test_indices)}")

    # 3) Extract triple counts (original-style cleaning)
    triple_counts = extract_triple_counts(train_df)

    # 4) Select top-N% triples and write summary
    select_top_triples(triple_counts, args.percent, args.summary_output)
    print(f"\nFinal summary file '{args.summary_output}' created.")

    # 5) Load summary into RDF graph
    g = load_graph_from_nt(args.summary_output)

    # 6) Evaluate queries
    print("Starting query evaluation on the summary graph...")
    evaluate_queries(g, df_queries, test_indices, endpoint_url=args.endpoint)


if __name__ == "__main__":
    main()
