# Love-at-First-Sight (LFS)
**First Answers Without the Awkward Silence in Big Knowledge Graphs**

## Overview

The increasing availability of large Knowledge Graphs (KGs) such as **DBpedia**, **YAGO**, and **Wikidata** has made SPARQL endpoints the standard interface for data exploration. However, exploratory SPARQL queries are often expensive, returning very large result sets or timing out, which limits interactive analysis.

**Love-at-First-Sight (LFS)** addresses this problem by enabling users to obtain **fast, first answers** to exploratory queries.  
The system introduces **First-Sight Summaries (FSS)**: compact RDF summaries built from historical query logs that can answer many user queries with very low latency, before optionally falling back to the original SPARQL endpoint.

LFS offers:
- Fast first answers to exploratory SPARQL queries
- RDF summaries constructed under budget constraints
- Significant latency reductions compared to direct endpoint querying
- Seamless integration with live SPARQL endpoints

---

## System Components

The current version of LFS consists of two main scripts:

1. **Parser / Data Creator** – materializes triples from query logs by querying a SPARQL endpoint  
2. **LFS Evaluator & Summary Builder** – builds an RDF summary and evaluates query coverage and performance

---

## 1. LFS Data Creator (Parser)

The parser reads a query log containing **comma-separated triple patterns**, sends them to a SPARQL endpoint, and instantiates triples from the returned bindings.

### Script
```
parser_vldb_v1.py
```

### Usage
```
python parser_vldb_v1.py queryfile mfflag basefilename limit urlendpoint
```

### Parameters

- **queryfile**  
  Input file containing one query per line.  
  Each line consists of comma-separated triple patterns.

- **mfflag**  
  Determines how bindings are selected:
  - `1` → use most frequent bindings
  - `0` → use bindings in endpoint order

- **basefilename**  
  Base name for all output files.

- **limit**  
  SPARQL `LIMIT` applied to each query.

- **urlendpoint**  
  SPARQL endpoint URL  
  (e.g. `https://yago-knowledge.org/sparql/query`)

### Output Files

For each run, the parser generates:

- `1{base}_{mfflag}_{limit}.txt` – top-1 instantiations  
- `5{base}_{mfflag}_{limit}.txt` – top-5 instantiations  
- `10{base}_{mfflag}_{limit}.txt` – top-10 instantiations  
- `Queries_{base}_{mfflag}_{limit}.txt` – query patterns that produced results

These files are used as input to the LFS evaluator.

---

## 2. LFS Evaluator & Summary Builder

This component constructs an **LFS RDF summary** from instantiated triples and evaluates how well it answers unseen queries.

### Script
```
LFS_vldb_v1.py
```

### Usage
```
python LFS_vldb_v1.py summary_file queries_file summary_output percent [--endpoint URL] [--novel-only]
```

### Parameters

- **summary_file**  
  File containing instantiated triples (parser output).

- **queries_file**  
  File containing query patterns to be evaluated.

- **summary_output**  
  Output `.nt` file containing the LFS summary.

- **percent**  
  Fraction of distinct triples to include in the summary  
  (e.g. `1.0` = 100%, `0.1` = 10%)

- **--endpoint (optional)**  
  SPARQL endpoint URL used when queries cannot be answered locally.

- **--novel-only (optional)**  
  Evaluate only test queries not seen during training.

### Evaluation Process

The evaluator:
1. Splits data into training and test sets (K-Fold)
2. Extracts and cleans triples from training data
3. Selects the most frequent triples
4. Builds an RDF summary in N-Triples format
5. Executes test queries against the summary
6. Optionally queries the remote endpoint
7. Reports coverage and timing statistics

---

## Example Workflow

```bash
# Step 1: Generate instantiated triples
python parser_vldb_v1.py YAGO_orig_queries.txt 1 yago 500 https://yago-knowledge.org/sparql/query

# Step 2: Build and evaluate an LFS summary
python LFS_vldb_v1.py 10yago_1_500.txt Queries_yago_1_500.txt yago_summary.nt 0.1     --endpoint https://yago-knowledge.org/sparql/query
```

---

## Datasets

Example outputs for **DBpedia**, **YAGO**, and **Wikidata** are provided in the `data/` directory and can be used directly with the LFS evaluator.

---

## Requirements

- Python 3.9 or newer

### Python Libraries
```
rdflib
pandas
numpy
SPARQLWrapper
scikit-learn
```

---

## Citation

If you use this software in your research, please cite the **Love-at-First-Sight (LFS)** paper.
