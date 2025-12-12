from rdflib.plugins.sparql.parser import parseQuery
from rdflib.plugins.sparql.algebra import translateQuery, CompValue
from rdflib.term import URIRef, Variable, BNode, Literal

# Global map: Variable -> bound term from VALUES
VALUES_MAP = {}


def format_term(t):
    """
    Convert rdflib terms into the desired string format:
    - Variables → ?var
    - URIs      → <uri>
    - Literals  → "value"
    - BNodes    → _:bnode
    """
    if isinstance(t, Variable):
        return f"?{t}"
    if isinstance(t, URIRef):
        return f"<{str(t)}>"
    if isinstance(t, Literal):
        # You can extend this to add datatype/lang if needed
        return f"\"{str(t)}\""
    if isinstance(t, BNode):
        return f"_:{str(t)}"
    return str(t)


def apply_values(term):
    """
    If the term is a Variable and appears in VALUES_MAP,
    return the bound value; otherwise return the term itself.
    """
    if isinstance(term, Variable) and term in VALUES_MAP:
        return VALUES_MAP[term]
    return term


def extract_triples_from_sparql(query_str):
    """
    Parse a SPARQL query string and extract only the **non-optional**
    triple patterns from the algebra, while also collecting VALUES()
    bindings into VALUES_MAP.
    """
    try:
        parsed = parseQuery(query_str)
        algebra = translateQuery(parsed)
    except Exception:
        # If parsing/algebra translation fails, just return no triples
        return []

    triples = []
    value_bindings = {}

    # --------- 1) Collect VALUES bindings ---------
    def collect_values(node):
        if isinstance(node, CompValue):
            if node.name == "Values":
                # Different rdflib versions store rows differently.
                # Commonly, rows are in node["p"] as a list of dicts:
                #   { Variable("x"): term, ... }
                rows = []
                if "p" in node:
                    rows = node.get("p", [])
                elif "P" in node:
                    rows = node.get("P", [])

                for row in rows:
                    if isinstance(row, dict):
                        for var, val in row.items():
                            if isinstance(var, Variable) and val is not None:
                                value_bindings[var] = val

            # Recurse into all children
            for key, val in node.items():
                if isinstance(val, (list, tuple)):
                    for v in val:
                        collect_values(v)
                else:
                    collect_values(val)

        elif isinstance(node, (list, tuple)):
            for x in node:
                collect_values(x)

    # --------- 2) Walk algebra and collect non-optional triples ---------
    def walk(node):
        if isinstance(node, CompValue):
            # OPTIONAL patterns are represented as LeftJoin in the algebra.
            if node.name == "LeftJoin":
                # Only walk the mandatory side (usually "p1" or "p")
                # and skip the optional part ("p2").
                for key, val in node.items():
                    if key in ("p2", "expr"):
                        continue
                    walk(val)
                return

            # Basic Graph Pattern: collect its triples
            if node.name == "BGP":
                for s, p, o in node.get("triples", []):
                    triples.append((s, p, o))

            # Generic recursion for all other algebra nodes
            for key, val in node.items():
                if isinstance(val, (list, tuple)):
                    for v in val:
                        walk(v)
                else:
                    walk(val)

        elif isinstance(node, (list, tuple)):
            for x in node:
                walk(x)

    # Run both passes
    base = algebra.algebra if hasattr(algebra, "algebra") else algebra
    collect_values(base)
    walk(base)

    # Store bindings globally so triple_to_str can see them
    global VALUES_MAP
    VALUES_MAP = value_bindings

    return triples


def triple_to_str(triple):
    """
    Convert a (s, p, o) triple into the desired string format,
    applying VALUES bindings first.
    """
    s, p, o = triple
    s = apply_values(s)
    p = apply_values(p)
    o = apply_values(o)
    return f"{format_term(s)} {format_term(p)} {format_term(o)}"


def process_file(input_file, output_file):
    """
    Read SPARQL queries from input_file (one per line),
    write comma-separated triples for each query to output_file.
    """
    with open(input_file, "r", encoding="utf-8") as fin, \
         open(output_file, "w", encoding="utf-8") as fout:

        for line in fin:
            query = line.strip()
            if not query:
                fout.write("\n")
                continue

            triples = extract_triples_from_sparql(query)
            triple_strings = [triple_to_str(t) for t in triples]
            fout.write(",".join(triple_strings) + "\n")


if __name__ == "__main__":
    # Change these paths to your actual files if needed
    input_file = "LSQDBqueries"
    output_file = "LSQDBQueriestoParse"

    process_file(input_file, output_file)
    print(f"Done. Wrote triples to {output_file}")
