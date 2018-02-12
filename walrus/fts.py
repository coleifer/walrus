from walrus.query import Executor
from walrus.query import OP_MATCH
from walrus.query import parse
from walrus.search import Tokenizer


class Index(object):
    def __init__(self, db, name, **tokenizer_settings):
        self.db = db
        self.name = name
        self.tokenizer = Tokenizer(**tokenizer_settings)

    def get_key(self, word):
        return self.db.ZSet('fts.%s.%s' % (self.name, word))

    def get_document_hash(self, document_id):
        return self.db.Hash('doc.%s.%s' % (self.name, document_id))

    def add(self, key, content, **metadata):
        document_hash = self.get_document_hash(key)
        document_hash.update(content=content, **metadata)

        for word, score in self.tokenizer.tokenize(content).items():
            word_key = self.get_key(word)
            word_key[key] = score

    def remove(self, key):
        document_hash = self.get_document_hash(key)
        content = document_hash['content']
        document_hash.clear()
        for word in self.tokenizer.tokenize(content):
            word_key = self.get_key(word)
            del word_key[key]
            if len(word_key) == 0:
                word_key.clear()

    def update(self, key, content, **metadata):
        self.remove(key)
        self.add(key, content, **metadata)

    def get_index(self, op):
        assert op == OP_MATCH
        return self

    def db_value(self, value):
        return value

    def search(self, expression):
        query = parse(expression, self)
        executor = Executor(self.db)
        return executor.execute(query)
