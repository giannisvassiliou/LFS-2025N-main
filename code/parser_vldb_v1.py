#!/usr/bin/env python3
"""
Batch SPARQL triple instantiator.

Reads a file of comma-separated triple patterns (one line per query),
sends each line as a SELECT query to a SPARQL endpoint, and writes out
instantiated triples for the top-1, top-5 and top-10 result bindings.

Output files (per run):

    1{base}_{mfflag}_{limit}.txt      # top-1 expansion
    5{base}_{mfflag}_{limit}.txt      # top-5 expansion
    10{base}_{mfflag}_{limit}.txt     # top-10 expansion
    Queries_{base}_{mfflag}_{limit}.txt  # original query patterns that produced output

mfflag:
    1 -> use most frequent result bindings
    0 -> use results in the order returned by the endpoint
"""

import argparse
import time
from collections import Counter
from typing import List, Dict

from SPARQLWrapper import SPARQLWrapper, JSON

totc = 0  # global counter, preserved from original script for compatibility


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Batch SPARQL triple instantiator."
    )
    parser.add_argument("queryfile", help="Input file with comma-separated triple patterns per line.")
    parser.add_argument(
        "mfflag",
        type=int,
        choices=[0, 1],
        help="0 = use first results (no most-frequent), 1 = use most frequent results.",
    )
    parser.add_argument("basefilename", help="Base filename for outputs.")
    parser.add_argument("limit", type=int, help="SPARQL LIMIT for each query.")
    parser.add_argument("urlendpoint", help="SPARQL endpoint URL.")
    return parser.parse_args()


def build_sparql_query(triple_line: str, limit: int) -> str:
    """
    Turn a line of comma-separated triple patterns into a SPARQL SELECT query.
    """
    triples = triple_line.split(",")
    triple_patterns = []

    for triple in triples:
        if triple.strip() == "":
            continue
        split_triple = triple.strip().split()
        if len(split_triple) < 3:
            continue
        s, p, o = split_triple[0], split_triple[1], split_triple[2]
        triple_patterns.append(f"{s} {p} {o} .")

    if not triple_patterns:
        return ""

    where_clause = " ".join(triple_patterns)
    query = f"SELECT * WHERE {{ {where_clause} }} LIMIT {limit}"
    return query


def query_endpoint(query: str, endpoint_url: str) -> List[Dict[str, Dict[str, str]]]:
    """
    Execute a SPARQL SELECT query and return the list of bindings.
    """
    sparql = SPARQLWrapper(endpoint_url)
    sparql.setReturnFormat(JSON)
    sparql.setQuery(query)
    result = sparql.queryAndConvert()
    return result.get("results", {}).get("bindings", [])


def instantiate_triples_for_binding(
    triples: List[str],
    binding: Dict[str, Dict[str, str]],
) -> str:
    """
    Given a list of triple patterns and a single result binding, return a string
    with instantiated triples separated by tabs.

    Example output (accou):
        "<A> <p> <B>\t<B> <q> <C>\t"
    """
    accou_parts: List[str] = []

    for triple in triples:
        triple = triple.strip()
        if triple == "":
            continue

        split_triple = triple.split()
        if len(split_triple) < 3:
            continue

        new_triple_parts: List[str] = []

        for term in split_triple:
            if "?" in term:
                # Variable: strip leading '?'
                var_name = term.lstrip("?")
                value_info = binding.get(var_name)

                if not value_info:
                    # No value for this variable in this binding
                    continue

                value = value_info.get("value", "")
                # Match original behavior: wrap in <>, remove tabs, replace spaces with underscores
                value_clean = value.replace("\t", "").replace(" ", "_")
                new_triple_parts.append(f"<{value_clean}>")
            else:
                # Constant term: keep as-is but clean whitespace/tabs
                const_clean = term.replace("\t", "").replace(" ", "_")
                new_triple_parts.append(const_clean)

        if new_triple_parts:
            new_triple = " ".join(new_triple_parts).strip()
            accou_parts.append(new_triple)

    # The original code added "\t" after each triple
    return "\t".join(accou_parts) + ("\t" if accou_parts else "")


def collect_instantiations(
    triple_line: str,
    bindings: List[Dict[str, Dict[str, str]]],
) -> List[str]:
    """
    For a line of triple patterns and all bindings, return the list of 'accou'
    strings (one per binding).
    """
    triples = triple_line.split(",")
    all_list: List[str] = []

    for binding in bindings:
        accou = instantiate_triples_for_binding(triples, binding)
        if accou:
            all_list.append(accou)

    return all_list


def get_top_concatenated(
    entries: List[str],
    mfflag: int,
    k: int,
) -> str:
    """
    Return a single string made by concatenating the top-k entries.

    If mfflag == 1, use most frequent entries (by frequency).
    If mfflag == 0, use entries in original order.

    Concatenation is done exactly as original code (just string concatenation).
    """
    if not entries:
        return ""

    if mfflag == 1:
        ordered = [key for key, _ in Counter(entries).most_common()]
    else:
        ordered = entries

    # Take top-k and concatenate them exactly as before
    top_k = ordered[:k]
    return "".join(top_k)


def clean_line_for_output(line: str) -> str:
    """
    Clean the original input line to match original output behavior.
    """
    lin = line.strip()
    line2 = lin.replace(",\n", "\n").replace("\n", "").replace("\r", "")
    line2 = line2.rstrip(",")
    return line2


def clean_instantiation_string(s: str) -> str:
    """
    Clean instantiation strings to match original replacements.
    """
    return s.replace(",\n", "\n").replace("\n", "").replace("\r", "")


def main() -> None:
    global totc

    args = parse_args()

    queryfile = args.queryfile
    mfflag = args.mfflag
    basefilename_orig = args.basefilename
    limit = args.limit
    endpoint_url = args.urlendpoint

    print(f"QUERY LOG: {queryfile}")
    print(f"MF FLAG: {mfflag}")
    print(f"BASEFILENAME: {basefilename_orig}")
    print(f"LIMIT: {limit}")
    print(f"ENDPOINT URL: {endpoint_url}")

    # In the original script, basefilename is modified once per run
    basefilename = f"{basefilename_orig}_{mfflag}_{limit}"

    # Open output files
    ff1 = open(f"1{basefilename}.txt", "w", encoding="utf-8")
    ff10 = open(f"10{basefilename}.txt", "w", encoding="utf-8")
    ff5 = open(f"5{basefilename}.txt", "w", encoding="utf-8")
    ff_queries = open(f"Queries_{basefilename}.txt", "w", encoding="utf-8")

    start_time = time.time()

    no = 0
    success_count = 0
    fail_count = 0

    with open(queryfile, encoding="utf-8") as f_in:
        for line in f_in:
            no += 1
            if no % 1000 == 0:
                print(no)

            # Skip empty and specific lines
            if not line.strip():
                continue
            if "ASKWHERE" in line or "CONSTRUCT" in line:
                continue

            # Build query
            sparql_query = build_sparql_query(line, limit)
            if not sparql_query:
                continue

            #print(sparql_query)

            try:
                bindings = query_endpoint(sparql_query, endpoint_url)
                #print(bindings)

                if not bindings:
                    fail_count += 1
                    continue

                all_list = collect_instantiations(line, bindings)
                if not all_list:
                    fail_count += 1
                    continue

                # Compute got1, got5, got10 as in original
                got1 = get_top_concatenated(all_list, mfflag, 1)
                got5 = get_top_concatenated(all_list, mfflag, 4)
                got10 = get_top_concatenated(all_list, mfflag, 9)

                line2 = clean_line_for_output(line)
                got1_clean = clean_instantiation_string(got1)
                got5_clean = clean_instantiation_string(got5)
                got10_clean = clean_instantiation_string(got10)

                #print("\nTHANASIS")
                print(got1_clean)

                # Match original check:
                if (
                    line2 != "?s ?p ?o ,"
                    and got10_clean != ""
                    and got5_clean != ""
                ):
                    print(f"{no} {totc}")
                    totc += 1

                    ff_queries.write(line2 + "\n")
                    ff5.write(got5_clean + "\n")
                    ff10.write(got10_clean + "\n")
                    ff1.write(got1_clean + "\n")

                    success_count += 1
                else:
                    fail_count += 1

            except Exception:
                # Keep same behavior: swallow exception but count it
                import traceback

                print("Exception while querying endpoint:")
                print(traceback.format_exc())
                fail_count += 1

    end_time = time.time()

    ff1.close()
    ff5.close()
    ff10.close()
    ff_queries.close()

    print(f"TIME start-end {end_time - start_time:.2f} seconds")
    print(f"Successful queries: {success_count}")
    print(f"Failed/empty queries: {fail_count}")
    print(f"Total rows processed: {no}")


if __name__ == "__main__":
    main()
