.. _full-text-search:

Full-text Search
================

Walrus comes with a standalone full-text search index that supports:

* Storing documents along with arbitrary metadata.
* Complex search using boolean/set operations and parentheses.
* Stop-word removal.
* Porter-stemming.
* Optional double-metaphone for phonetic search.

To create a full-text index, use:

* :py:meth:`Database.Index`
* :py:class:`Index`

Example:

.. code-block:: python

    from walrus import Database

    db = Database()
    search_index = db.Index('app-search')

    # Phonetic search.
    phonetic_index = db.Index('phonetic-search', metaphone=True)

Storing data
------------

Use the :py:meth:`Index.add` method to add documents to the search index:

.. code-block:: python

    # Specify the document's unique ID and the content to be indexed.
    search_index.add('doc-1', 'this is the content of document 1')

    # Besides the document ID and content, we can also store metadata, which is
    # not searchable, but is returned along with the document content when a
    # search is performed.
    search_index.add('doc-2', 'another document', title='Another', status='1')

To update a document, use either the :py:meth:`Index.update` or
:py:meth:`Index.replace` methods. The former will update existing metadata
while the latter clears any pre-existing metadata before saving.

.. code-block:: python

    # Update doc-1's content and metadata.
    search_index.update('doc-1', 'this is the new content', title='Doc 1')

    # Overwrite doc-2...the "status" metadata value set earlier will be lost.
    search_index.replace('doc-2', 'another document', title='Another doc')

To remove a document use :py:meth:`Index.remove`:

.. code-block:: python

    search_index.remove('doc-1')  # Removed from index and removed metadata.

Searching
---------

Use the :py:meth:`Index.search` method to perform searches. The search query
can include set operations (e.g. *AND*, *OR*) and use parentheses to indicate
operation precedence.

.. code-block:: python

    for document in search_index.search('python AND flask'):
        # Print the "title" that was stored as metadata. The "content" field
        # contains the original content of the document as it was indexed.
        print(document['title'], document['content'])

Phonetic search, using ``metaphone``, is tolerant of typos:

.. code-block:: python

    for document in phonetic_index.search('flasck AND pythonn'):
        print(document['title'], document['content'])

For more information, see the :py:class:`Index` API documentation.
