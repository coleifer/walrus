import re

from walrus.utils import decode
from walrus.search import Tokenizer


# Quoted phrases, operators, words and parentheses. Anything else, such as
# punctuation, is skipped.
_scanner = re.Scanner([
    (r'"[^\n"\\]*(?:\\.[^\n"\\]*)*"', lambda s, t: ('WORDS', t[1:-1])),
    (r"'[^\n'\\]*(?:\\.[^\n'\\]*)*'", lambda s, t: ('WORDS', t[1:-1])),
    (r'\bAND\b|&', lambda s, t: ('AND', t)),
    (r'\bOR\b|\|', lambda s, t: ('OR', t)),
    (r'[@_\-\w]+', lambda s, t: ('WORDS', t)),
    (r'\(', lambda s, t: ('(', t)),
    (r'\)', lambda s, t: (')', t)),
    (r'\s+|.', None),
], re.U)


def _lex(query):
    tokens, last = [], None
    for kind, value in _scanner.scan(query)[0]:
        # Adjacent words and groups are joined by an implied AND.
        if kind in ('WORDS', '(') and last in ('WORDS', ')'):
            tokens.append(('AND', None))
        tokens.append((kind, value))
        last = kind
    return tokens


def _doc_id(key):
    return decode(key) if isinstance(key, bytes) else str(key)


class Index(object):
    """
    Full-text search index.

    Store documents, along with arbitrary metadata, and perform full-text
    search on the document content. Supports porter-stemming, stopword
    filtering, basic result ranking, and (optionally) double-metaphone for
    phonetic search.
    """
    def __init__(self, db, name, **tokenizer_settings):
        """
        :param Database db: a walrus database object.
        :param str name: name for the search index.
        :param bool stemmer: use porter stemmer (default True).
        :param bool metaphone: use double metaphone (default False).
        :param str stopwords_file: defaults to walrus stopwords.txt.
        :param int min_word_length: specify minimum word length.

        Create a search index for storing and searching documents.
        """
        self.db = db
        self.name = name
        self.tokenizer = Tokenizer(**tokenizer_settings)
        self.members = self.db.Set('fts.%s' % self.name)

    def get_key(self, word):
        return self.db.ZSet('fts.%s.%s' % (self.name, word))

    def _get_hash(self, document_id):
        return self.db.Hash('doc.%s.%s' % (self.name, _doc_id(document_id)))

    def get_document(self, document_id):
        """
        :param document_id: Document unique identifier.
        :returns: a dictionary containing the document content and
                  any associated metadata.
        """
        return self._get_hash(document_id).as_dict(True)

    def add(self, key, content, __metadata=None, **metadata):
        """
        :param key: Document unique identifier.
        :param str content: Content to store and index for search.
        :param metadata: Arbitrary key/value pairs to store for document.

        Add a document to the search index. Adding an existing key replaces
        its content and merges in the new metadata.
        """
        key = _doc_id(key)
        if key in self.members:
            self.remove(key, preserve_data=True)

        self.members.add(key)
        self._get_hash(key).update(dict(__metadata or {}, **metadata),
                                   content=content)

        for word, score in self.tokenizer.tokenize(content).items():
            word_key = self.get_key(word)
            word_key[key] = -score

    def remove(self, key, preserve_data=False):
        """
        :param key: Document unique identifier.

        Remove the document from the search index.
        """
        key = _doc_id(key)
        if self.members.remove(key) != 1:
            raise KeyError('Document with key "%s" not found.' % key)
        document_hash = self._get_hash(key)
        content = decode(document_hash['content'])
        if not preserve_data:
            document_hash.clear()

        for word in self.tokenizer.tokenize(content):
            word_key = self.get_key(word)
            del word_key[key]
            if len(word_key) == 0:
                word_key.clear()

    def update(self, key, content, __metadata=None, **metadata):
        """
        :param key: Document unique identifier.
        :param str content: Content to store and index for search.
        :param metadata: Arbitrary key/value pairs to store for document.

        Update the given document. Existing metadata will be preserved and,
        optionally, updated with the provided metadata.
        """
        self.remove(key, preserve_data=True)
        self.add(key, content, __metadata, **metadata)

    def replace(self, key, content, __metadata=None, **metadata):
        """
        :param key: Document unique identifier.
        :param str content: Content to store and index for search.
        :param metadata: Arbitrary key/value pairs to store for document.

        Update the given document. Existing metadata will not be removed and
        replaced with the provided metadata.
        """
        self.remove(key)
        self.add(key, content, __metadata, **metadata)

    # Each part of a query becomes a temporary sorted set of matching document
    # keys, scored so that better matches sort first, or None when the part
    # has no searchable words.
    def _store(self, command, keys):
        result_key = self.db.get_temp_key()
        getattr(self.db, command)(result_key, keys)
        self.db.expire(result_key, 15)
        return result_key

    def _match(self, words):
        keys = [self.get_key(word).key
                for word in self.tokenizer.tokenize(words)]
        # Stopwords and short words are not indexed, so they are left out.
        return self._store('zinterstore', keys) if keys else None

    def _combine(self, command, lhs, rhs):
        if lhs is None or rhs is None:
            return rhs if lhs is None else lhs
        return self._store(command, [lhs, rhs])

    def _search(self, query):
        tokens = _lex(query)
        if not tokens:
            return [(member, 0) for member in self.members]

        def accept(kind):
            if tokens and tokens[0][0] == kind:
                return tokens.pop(0)

        def expression():
            # OR binds less tightly than AND.
            result = term()
            while accept('OR'):
                result = self._combine('zunionstore', result, term())
            return result

        def term():
            result = factor()
            while accept('AND'):
                result = self._combine('zinterstore', result, factor())
            return result

        def factor():
            words = accept('WORDS')
            if words:
                return self._match(words[1])
            elif accept('('):
                result = expression()
                accept(')')  # Tolerate a missing close at the end.
                return result
            raise ValueError('Malformed query: %s' % query)

        result_key = expression()
        if tokens:
            raise ValueError('Malformed query: %s' % query)
        return self.db.ZSet(result_key) if result_key else []

    def search(self, query):
        """
        :param str query: Search query. May contain boolean/set operations
            and parentheses.
        :returns: a list of document hashes corresponding to matching
            documents.

        Search the index. The return value is a list of dictionaries
        corresponding to the documents that matched. These dictionaries contain
        a ``content`` key with the original indexed content, along with any
        additional metadata that was specified.
        """
        return [self.get_document(key) for key, _ in self._search(query)]

    def search_items(self, query):
        """
        :param str query: Search query. May contain boolean/set operations
            and parentheses.
        :returns: a list of (key, document hashes) tuples corresponding to
            matching documents.

        Search the index. The return value is a list of (key, document dict)
        corresponding to the documents that matched. These dictionaries contain
        a ``content`` key with the original indexed content, along with any
        additional metadata that was specified.
        """
        return [(decode(key), self.get_document(key))
                for key, _ in self._search(query)]
