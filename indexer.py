import sys
import os
import xml.sax
import pickle
from xml_processor import WikiParser, output_dump_path


def linear_merge(output, ip1, ip2):
    with open(output, 'w') as m, open(ip1) as f1, open(ip2) as f2:
        line1 = f1.readline()
        line2 = f2.readline()

        while line1 or line2:

            if not line1:
                m.write(line2)
                line2 = f2.readline()
            elif not line2:
                m.write(line1)
                line1 = f1.readline()

            else:
                w1 = line1.split()[0]
                w2 = line2.split()[0]

                if w2 < w1:
                    m.write(line2)
                    line2 = f2.readline()
                elif w1 < w2:
                    m.write(line1)
                    line1 = f1.readline()
                elif w1 == w2:
                    l = ' '.join([w1] + line1.split()
                                 [1:] + line2.split()[1:])
                    m.write(l + '\n')
                    line1 = f1.readline()
                    line2 = f2.readline()


def merge_indexes(out_path, dir_path, num_files):
    l = 1
    r = num_files
    while r != 1:
        cnt = 0
        for i in range(l, r, 2):
            cnt += 1
            out_file = f"{dir_path}/v_{cnt}.txt"
            ip1_file = f"{dir_path}/index_{i}"
            if i + 1 > r:
                os.rename(ip1_file, f"{dir_path}/index_{cnt}.txt")
                os.remove(ip1_file)
                break
            ip2_file = f"{dir_path}/index_{i + 1}"

            linear_merge(out_file, ip1_file, ip2_file)

            os.remove(ip1_file)
            os.remove(ip2_file)
            os.rename(out_file, f"{dir_path}/index_{cnt}.txt")
        r = cnt
    os.rename(f"{dir_path}/index_1.txt", f"{out_path}/index.txt")


def create_indexes(wiki_dump_path):
    # The content handler
    wiki_parser = WikiParser()

    # The XML parser
    parser = xml.sax.make_parser()
    parser.setContentHandler(wiki_parser)

    # creating the folder to store index files
    try:
        os.mkdir('data_dump')
    except FileExistsError:
        pass

    # creating the folder to store main files
    try:
        os.mkdir('data')
    except FileExistsError:
        pass

    # parsing the XML document
    with open(wiki_dump_path, 'r') as f:
        for line in f:
            parser.feed(line)
    wiki_parser.write_last_batch()
    wiki_parser.write_title_list('data')

    return wiki_parser.batches


def main():
    n = len(sys.argv)
    if n != 3:
        print("Invalid Command. Usage: python3 indexer.py <path_to_wiki_dump> <path_to_inverted_index>")
        exit()

    wiki_dump_path = sys.argv[1]
    inverted_index_path = sys.argv[2]

    num_files = create_indexes(wiki_dump_path)
    merge_indexes(inverted_index_path, output_dump_path, num_files)


if __name__ == '__main__':
    main()
