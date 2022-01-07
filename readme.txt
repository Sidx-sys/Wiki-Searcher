## Files
1. xml_processor.py -> Has the main class for processing the xml document and creating the indexes. For every 10000
pages read, its the index is written on to a file.

2. indexer.py -> Has the task to create the index files using the xml_processor class and then merge them to form a
single inverted index file, which can be stored in folder specified at execution.

3. reducer.py -> Has the task to compress the created index file to a comparitively smaller index file by filtering
the not so important postings from postings list of the tokens in the inverted index and then also creating the offset
file required for searching. It also stores the total number of documents in the inverted index required in
search (tfidf).

4. search.py -> Has the search functionality to process a given query file and output the query results. It requires
a data directory to be in the same directory as the file which contains the inverted index, an offset file for binary
searching, file with number of documents for tfidf and a title list file for returning the titles of the required
documents.

## Running instructions
python3 indexer.py <path_to_xml_dump> <path_to_inverted_index_folder>
python3 reducer.py <path_to_inverted_index>
python3 search.py <path_to_query_file>

## Assumptions
Assumption for running the search.py file is that the path for offset file, title_list file, num_docs 
file and inverted index file is specified correctly in the main function.
