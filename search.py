import sys
import os
import re
import pickle
import json
from math import log10
from time import perf_counter
from nltk.stem.snowball import SnowballStemmer
from nltk.corpus import stopwords

stemmer = SnowballStemmer(language='english')
stopwords_set = set(stopwords.words('english'))


class Searcher():
    def __init__(self, index_path, title_path, offset_path, num_docs_path):
        self._idx_map = ['T', 't', 'b', 'i', 'r', 'e', 'c']
        self._field_weights = {'t': 20, 'b': 1,
                               'i': 5, 'c': 1, 'r': 0.5, 'e': 0.5}
        with open(offset_path, 'rb') as f:
            self._offset_list = pickle.load(f)
        with open(title_path, 'rb') as f:
            self._title_list = pickle.load(f)
        with open(num_docs_path, 'rb') as f:
            self._total_docs = pickle.load(f)
        self._num_tokens = len(self._offset_list)
        self._index_path = index_path
        self._docs = {}

    def _process_token(self, token):
        token_r = re.search(r"[a-z0-9]+", token).group(0)

        result = ""
        if token_r not in stopwords_set:
            result = stemmer.stem(token_r)

        return result

    def _process_query(self, query):
        query = query.lower()
        query = re.sub(r"'", '', query)
        q_set = {}
        n = len(query)
        key = 'T'
        if n > 1 and query[1] != ':':
            q_set[key] = ''

        for i in range(n):
            cur = query[i]
            nxt = ''
            if cur == ':':
                continue
            if i < n - 1:
                nxt = query[i + 1]
            if nxt == ':' and cur not in q_set:
                q_set[cur] = ''
                key = cur
                continue
            q_set[key] += query[i]

        for key in q_set:
            q_set[key] = q_set[key].split()

        return q_set

    def search_token(self, token):
        f = open(self._index_path)
        to_search = self._process_token(token)

        if to_search == '':
            return []

        postings_list = []
        l = 0
        r = self._num_tokens - 1

        while l <= r:
            m = (l + r) // 2
            f.seek(self._offset_list[m])
            candidate = f.readline().split()
            x = candidate[0]

            if x == to_search:
                postings_list = candidate[1:]
                break
            elif x > to_search:
                r = m - 1
            else:
                l = m + 1

        return postings_list

    def _fill_docs(self, postings_list, field):
        for posting in postings_list:
            doc_id = int(re.search(r'(\d+)', posting).group(0))
            weighted_tfidf = 0
            idf = log10(self._total_docs / len(posting))

            if field == 'T':
                freq = [0] * 7
                for i in range(1, 7):
                    search_string = re.compile(f"{self._idx_map[i]}(\d+)")
                    count = re.search(search_string, posting)
                    if count != None:
                        freq[i] += int(count.group(1))
                        tf = self._field_weights[self._idx_map[i]] * freq[i]
                        weighted_tfidf += log10(1 + tf) * idf
            else:
                search_string = re.compile(f"{field}(\d+)")
                count = re.search(search_string, posting)
                if count != None:
                    tf = self._field_weights['t'] * int(count.group(1))
                    weighted_tfidf += log10(1 + tf) * idf
            if weighted_tfidf:
                if doc_id not in self._docs:
                    self._docs[doc_id] = 0
                self._docs[doc_id] += weighted_tfidf

    def _top_docs(self):
        sorted_docs = sorted(
            self._docs.items(), key=lambda v: v[1], reverse=True)
        return [(doc_id, self._title_list[doc_id - 1]) for doc_id, _ in sorted_docs[:10]]

    def resolve_query(self, query):
        self._docs = {}
        q_set = self._process_query(query)
        for field in q_set:
            for token in q_set[field]:
                postings_list = self.search_token(token)
                self._fill_docs(postings_list, field)
        results = self._top_docs()
        return results


def main():
    n = len(sys.argv)
    if n != 2:
        print("Invalid Command. Usage: python3 search.py <query.txt>")
        exit()

    index_path = 'data/index.txt'
    offset_path = 'data/offset.pkl'
    title_path = 'data/title_list.pkl'
    num_docs_path = 'data/num_docs.pkl'
    query_file = sys.argv[1]

    queries = []
    with open(query_file) as f:
        queries = [line.rstrip() for line in f.readlines()]

    searcher = Searcher(index_path, title_path, offset_path, num_docs_path)
    results = []
    time = []
    for query in queries:
        tic = perf_counter()
        result = searcher.resolve_query(query)
        toc = perf_counter()
        results.append({'res': result, 'time': toc - tic})

    with open('queries_op.txt', 'w') as f:
        for result in results:
            for doc_id, title in result['res']:
                f.write(f"{doc_id}, {title}\n")
            f.write(f"{result['time']:0.4f}\n")
            f.write('\n')


if __name__ == '__main__':
    main()
