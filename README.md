# Love-at-First-Sight:First-Sight Summaries for Rapid Query Answering  in Big Knowledge Graphs
The increasing number of large knowledge graphs now available
online requires methods for their effective and efficient exploration.
Most of these knowledge graphs offer online SPARQL endpoints for
querying and exploring their data. In a typical scenario, the users
issue coarse exploratory queries at the beginning, refining them
further in the sequel in order to find the answer to the question
in mind. However, those coarse exploratory queries are costly to
evaluate as they usually involve many results and take too much
time to be answered, or even worse they time out, limiting the
exploration potential of the data they expose.
In this paper, we present the LFS (Love-at-First-Sight) system,
offering a unique solution to the aforementioned problem, enabling
users to get the first answers to their queries rapidly. More specifically we are the first to define the problem of constructing a firstsight summary, i.e., a summary able to provide rapidly the first
answers to user queries. We provide effective algorithms for their
construction, relying on existing query logs, and we both analytically and experimentally show the big benefits of the proposed
summaries. They have a compact size, and they can efficiently
provide first answers (even to unseen) user queries, improving by
orders of magnitude query response times
 <p align="center">
 
</p>
<p align="center">
  <img src="https://github.com/giannisvassiliou/LFS-ICDE-2024/blob/main/lfs2.JPG?raw=true" alt="Sublime's custom image"/>
</p>

## LFS Data Creator

Firstly we need to create the data by accessing the endpoint of the desired dataset. The parserIT.py script has the following syntax
<br>
<br><b> USAGE:  parserIT queryfile {flag 1/0 m(ost) f(requent) or non mf}  {basefilename} {limit} {urlendpoint} </b>
<br>
<br>
<b>
Where
</b>
<li>
queryfile : The name of the original query log (see data folder, choose e.g YAGO_orig_quer.txt) 
</li>
<li>
flag : 0 or 1  whether we need most frequent results 1 (yes) or  0 (no)
</li>
<li>
basefilename: a string to base the output file names {e.g yago}
</li>



<li>
limit: a SPARQL limit {e.g. 500}
</li>
<li>
urlendpoint: a valid url endpoint ( e.g.  https://yago-knowledge.org/sparql/query )
</li>
<br>
<b> We have stored in data folder results-examples for the 3 datasets we used (DBpedia, YAGO, Wikidata) </b>
<br> These files can be used directly from the LFS Evaluator

<br> <br>
## LFS Evaluator

You need to provide two INPUT files (<b> orig_summary_filename</b> and <b> queries_for_summary</b> ) and one filename for OUTPUT (the actual <b> .nt LFS Summary </b>) ,  finally  the <b> address_of_endpoint </b>{OPTIONAL}
<br><b> <br>
USAGE:  lfs orig_summary_filename queries_for_summary LFS_summary_output {url of endpoint - optional} </b>
<br>
<br>
<b>
Where
</b>
<li>
 orig_summary_filename: The filename of the summary that parserIT produced
 </li>
 <li>
 queries_for_summary: The filename of the previous summary, corresponding queries
 
</li>
<li>
 LFS_summary_output: The final .nt file of the actual LFS summary
</li>

<li> address_of_endpoint: if given, the system will try to evaluate the queries cannot be answered by the LFS Summary, from the endpoint
</li>

<br>

<b>The previous script, will:</b>
<br>
<li> Create the train/test portions from the orig_summary_filename </li>
<li> Create the lfs .nt summary (from the train portion)</li>
<li> Query the .nt summary created with the test queries</li>
<li> Present the % of the first-sight queries replied, and the time consumed</li>

 ## Used Python v3.9 - Required Python libraries
<br>
<li>rdflib</li>
<li>pandas</li>
<li>SPARQLWrapper</li>
<li>numpy</li>
<li>sys</li>
<li>JSON</li>
