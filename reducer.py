import os
import sys
import re
import pickle


class Compressor():
    def __init__(self, index_file):
        self.idx_map = ['T', 't', 'b', 'i', 'r', 'e', 'c']
        self.field_weights = {'t': 10, 'b': 1,
                              'i': 2, 'c': 0.75, 'r': 0.5, 'e': 0.5}
        self.index_file = index_file
        self._docs = set()

    def _parse_posting(self, posting):
        doc_id = int(re.search(r'(\d+)', posting).group(0))
        self._docs.add(doc_id)
        freq = [0] * 7
        weighted_tf = 0
        for i in range(1, 7):
            search_string = re.compile(f"{self.idx_map[i]}(\d+)")
            count = re.search(search_string, posting)
            if count != None:
                freq[i] += int(count.group(1))
                freq[0] += int(count.group(1))
                weighted_tf += self.field_weights[self.idx_map[i]] * freq[i]
        return freq, weighted_tf

    def _compress(self, postings_list):
        section = postings_list.split()
        tf_postings = []
        index = {}

        for posting in section[1:]:
            freq, weighted_tf = self._parse_posting(posting)
            tf_postings.append((weighted_tf, posting))
            index[posting] = freq

        tf_postings.sort(reverse=True)
        new_postings_list = [section[0]]
        counts = [0] * 7

        for _, posting in tf_postings:
            freq = index[posting]
            flag = False
            for i in range(1, 7):
                if freq[i] and counts[i] < 11000 and not flag:
                    new_postings_list.append(posting)
                    flag = True
                if flag and freq[i]:
                    counts[i] += 1
            if len(new_postings_list) > 66000:
                break

        return ' '.join(new_postings_list)

    def _write_num_docs(self):
        total_docs = len(self._docs)
        with open('data/num_docs.pkl', 'wb') as f:
            pickle.dump(total_docs, f)

    def start(self):
        inp = open(self.index_file, 'r')
        out = open('data/index.txt', 'w')
        cnt = 1

        for line in inp:
            if cnt % 1000000 == 0:
                print(cnt)
            cnt += 1

            compressed_postings_list = self._compress(line)
            out.write(compressed_postings_list + '\n')

        inp.close()
        out.close()
        self._write_num_docs()


def create_offset_file():
    f = open(f"data/index.txt", 'rb')
    offset = 0
    num_tokens = 0
    line = f.readline()
    offset_list = [0]
    while(line):
        num_tokens += 1
        offset += len(line)
        offset_list.append(offset)
        f.seek(offset)
        line = f.readline()
    f.close()

    with open(f'data/offset.pkl', 'wb') as f:
        pickle.dump(offset_list, f)

    return num_tokens


if __name__ == '__main__':
    args = sys.argv
    if len(args) != 2:
        print("Usage: python3 reducer.py <path_to_inverted_index>")
        exit()

    try:
        os.mkdir('data')
    except FileExistsError:
        pass

    compressor = Compressor(args[1])
    compressor.start()
    total_tokens_inverted_index = create_offset_file()
    print(total_tokens_inverted_index)
