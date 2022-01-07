import xml.sax
import string
import re
import regex
from nltk.stem.snowball import SnowballStemmer
from nltk.corpus import stopwords

stemmer = SnowballStemmer(language='english')
stopwords_set = set(stopwords.words('english'))

stemmer_cache = {}
stemmed_set = set()

idx_map = ['T', 't', 'b', 'i', 'r', 'e', 'c']
output_dump_path = 'data_dump'


def process_text(text):
    '''Function to process text for improved retrieval
        - tokenization
        - stop words removal
        - stemming
    '''
    text_tokenized = re.findall(r"\w+", text)

    result = []
    for token in text_tokenized:
        is_ascii = True
        try:
            token.encode('ascii')
        except UnicodeEncodeError:
            is_ascii = False

        if '_' not in token:
            if token not in stopwords_set and len(token) < 20 and is_ascii:
                stemmed = ""
                if token in stemmed_set:
                    stemmed = stemmer_cache[token]
                else:
                    stemmed = stemmer.stem(token)
                    stemmed_set.add(token)
                    stemmer_cache[token] = stemmed
                result.append(stemmed)

    return result


def get_title(text):
    return process_text(text)


def get_infobox(text):
    '''Extract the infobox from the document body'''
    idx = [m.end() for m in re.finditer(
        r'{{ ?infobox', text)]

    infobox_text = ""
    for i in idx:
        counter = 1
        for j in range(i + 1, len(text) - 1):
            if text[j: j + 2] == '}}':
                counter -= 1
            elif text[j: j + 2] == '{{':
                counter += 1
            if counter == 0:
                infobox_text += text[i + 1:j - 2]
                break

    return process_text(infobox_text)


def get_references(text):
    '''Extract the references from the document body'''
    idx = [m.end() for m in re.finditer(
        r'== ?references ?==', text)]

    reference_text = ""
    for i in idx:
        for j in range(i + 1, len(text) - 1):
            if text[j] == '=' or text[j:j + 2] == '[[':
                reference_text += text[i + 1:j] + ' '
                break

    return process_text(reference_text)


def get_links_category(text):
    '''Extract the links and categories from the document body'''
    idx = [m.end() for m in re.finditer(
        r'== ?external links ?==', text)]

    links_text = ""
    count = 0
    for i in idx:
        count = i
        for j in range(i + 1, len(text) - 1):
            if text[j: j + 2] == '[[' or text[j: j + 2] == '==':
                links_text += text[i + 1:j - 1] + ' '
                break
            count += 1
    if idx == []:
        count = 5
    new_text = text[count - 5:]
    idx = [m.end() for m in re.finditer(
        r'\[\[category', new_text)]

    category_text = ''
    for i in idx:
        for j in range(i + 1, len(new_text) - 1):
            if new_text[j: j + 2] == ']]':
                category_text += new_text[i + 1:j] + ' '
                break

    return process_text(links_text), process_text(category_text)


def get_body(text):
    '''Extract the body from the document text
       For speeding up the time removing unnecessary portions
       from entire text
    '''
    links_idx = re.search(r'== ?external links ?==', text)
    reference_idx = re.search(r'== ?references ?==', text)
    idx1 = len(text) if links_idx == None else links_idx.start()
    idx2 = len(text) if reference_idx == None else reference_idx.start()

    new_text = text[:min(idx1, idx2)]
    new_text = regex.sub(
        r'(?=\{\{ ?infobox)(\{\{(?:[^{}]|(?1))*\}\})', '', new_text)
    new_text = re.sub(r'\'\'\'', '', new_text)
    new_text = re.sub(r'<!--(.|\n)*?-->', '', new_text)

    return process_text(new_text)


class WikiParser(xml.sax.handler.ContentHandler):
    '''Content Handler for the SAX Parser'''

    def __init__(self):
        xml.sax.handler.ContentHandler.__init__(self)
        self._buffer = None
        self._values = {}
        self._current_tag = None
        self._i = 0
        self._found_id = False
        self.is_redirect = False
        self.index = {}
        self.batches = 0
        self.total_tokens = 0
        self._title_list = []

    def startElement(self, name, attrs):
        '''Opening tag of an element'''
        if name in ("title", "text"):
            self._current_tag = name
            self._buffer = []

        if name == "redirect":
            self.is_redirect = True

    def endElement(self, name):
        '''Ending tag of an element'''
        if name == self._current_tag:
            self._values[name] = ' '.join(self._buffer)
            self._current_tag = None

        if name == "page":
            title = self._values['title'].lower()
            text = self._values['text'].lower()

            if not self.is_redirect:
                self.title_list.append(title)

                title = re.sub(r"'", '', title)
                text = re.sub(r"'", '', text)

                document = []
                document.append(get_title(title))
                document.append(get_body(text))
                document.append(get_infobox(text))
                document.append(get_references(text))
                links, category = get_links_category(text)
                document.append(links)
                document.append(category)

                self._i += 1
                self._found_id = False
                self.index_document(self._i, document)

                if self._i % 10000 == 0:
                    self.write_index_batch(self._i//10000)
                    self.batches += 1
                    self.index = {}

            self.is_redirect = False

    def characters(self, content):
        '''Characters between opening and closing tag'''
        if self._current_tag:
            self._buffer.append(content)

    def index_document(self, doc_id, document):
        for i in range(6):
            field = i + 1
            for token in document[i]:
                if token not in self.index:
                    freq = [0]*7
                    freq[0] = 1
                    freq[field] = 1
                    self.index[token] = {}
                    self.index[token][doc_id] = freq

                elif doc_id not in self.index[token]:
                    freq = [0]*7
                    freq[0] = 1
                    freq[field] = 1
                    self.index[token][doc_id] = freq

                else:
                    self.index[token][doc_id][0] += 1
                    self.index[token][doc_id][field] += 1

    def write_index_batch(self, batch_num):
        file_name = f"{output_dump_path}/index_{batch_num}"
        f = open(file_name, 'a')

        for token in sorted(self.index):
            f.write(token)
            for doc_id in self.index[token]:
                freq = self.index[token][doc_id]
                doc_text = f" {doc_id}"
                for i in range(1, 7):
                    if freq[i] != 0:
                        doc_text += f"{idx_map[i]}{freq[i]}"
                f.write(doc_text)
            f.write('\n')
        f.close()

    def write_last_batch(self):
        if not self.index:
            return
        self.batches += 1
        file_name = f"{output_dump_path}/index_{self._i//10000 + 1}"
        f = open(file_name, 'a')
        for token in sorted(self.index):
            f.write(token)
            for doc_id in self.index[token]:
                freq = self.index[token][doc_id]
                doc_text = f" {doc_id}"
                for i in range(1, 7):
                    if freq[i] != 0:
                        doc_text += f"{idx_map[i]}{freq[i]}"
                f.write(doc_text)
            f.write('\n')
        f.close()

    def read_index_batch(self, batch_num):
        file_name = f"{output_dump_path}/index_{batch_num}"
        batch_index = {}
        with open(file_name) as f:
            for line in f:
                section_1 = line.split()
                batch_index[section_1[0]] = {}
                for section in section_1[1:]:
                    doc_id = int(re.search(r'(\d+)', section).group(0))
                    freq = [0]*7
                    for i in range(1, 7):
                        search_string = re.compile(f"{idx_map[i]}(\d+)")
                        count = re.search(search_string, section)
                        if count != None:
                            freq[i] += int(count.group(1))
                            freq[0] += int(count.group(1))
                    batch_index[section_1[0]][doc_id] = freq

        return batch_index

    def write_title_list(self, folder_location):
        file_name = f"{folder_location}/title_list.pkl"
        with open(file_name, 'wb') as f:
            pickle.dump(self._title_list, f)
