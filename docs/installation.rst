.. _installation:

Installing and Testing
======================

Most users will want to simply install the latest version, hosted on PyPI:

.. code-block:: console

    pip install walrus


Installing with git
-------------------

The project is hosted at https://github.com/coleifer/walrus and can be installed
using git:

.. code-block:: console

    git clone https://github.com/coleifer/walrus.git
    cd walrus
    pip install .

Running tests
-------------

You can test your installation by running the test suite. Requires a running Redis server.

.. code-block:: console

    python runtests.py
