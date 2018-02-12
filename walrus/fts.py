from walrus.query import Executor
from walrus.query import OP_MATCH
from walrus.query import parse
from walrus.utils import decode
from walrus.search import Tokenizer


class Index(object):
    def __init__(self, db, name, **tokenizer_settings):
        self.db = db
        self.name = name
        self.tokenizer = Tokenizer(**tokenizer_settings)
        self.members = self.db.Set('fts.%s' % self.name)

    def get_key(self, word):
        return self.db.ZSet('fts.%s.%s' % (self.name, word))

    def get_document(self, document_id):
        return self.db.Hash('doc.%s.%s' % (self.name, decode(document_id)))

    def add(self, key, content, **metadata):
        self.members.add(key)
        document_hash = self.get_document(key)
        document_hash.update(content=content, **metadata)

        for word, score in self.tokenizer.tokenize(content).items():
            word_key = self.get_key(word)
            word_key[key] = -score

    def remove(self, key, preserve_data=False):
        if self.members.remove(key) != 1:
            raise KeyError('Document with key "%s" not found.' % key)
        document_hash = self.get_document(key)
        content = decode(document_hash['content'])
        if not preserve_data:
            document_hash.clear()

        for word in self.tokenizer.tokenize(content):
            word_key = self.get_key(word)
            del word_key[key]
            if len(word_key) == 0:
                word_key.clear()

    def update(self, key, content, **metadata):
        self.remove(key, preserve_data=True)
        self.add(key, content, **metadata)

    def replace(self, key, content, **metadata):
        self.remove(key)
        self.add(key, content, **metadata)

    def get_index(self, op):
        assert op == OP_MATCH
        return self

    def db_value(self, value):
        return value

    def _search(self, query):
        expression = parse(query, self)
        if expression is None:
            return [(member, 0) for member in self.members]
        executor = Executor(self.db)
        return executor.execute(expression)

    def search(self, query):
        return [self.get_document(key) for key, _ in self._search(query)]
