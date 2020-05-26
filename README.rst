============================
Metapack Database Management
============================


The extension to Metapack can create SQL statements to load a Metapack
data package into a database.


Install
-------

After installing `metapack <http:docs.metatab.org>`_,

.. code-block:: bash

    $ pip install metapack-db

After installation, the ``mp config`` command should show the ``sql`` and ``db`
subcommands.


Running
-------

Only the ``mp sql`` command is currently implemented. The command takes
a reference to a metapack package, a list of packages, or a text file with a list
package references and outputs SQL commands for dropping, creating or loading
the resources of those packages.

The ``mp sql`` command takes three options to control what type of SQL is emitted:

* ``--create``, to create tables
* ``--drop``, to drop tables
* ``--load`` to load data for tables.

If none of these three are specified, they are all set, and the resulting SQL
will drop, create and load the data.

[ Although loading is not implemented for all dialects.

Loading a single package into a Sqlite database::

     mp sql --sqlite metapack+http://library.metatab.org/cde.ca.gov-reimbursements-1.csv | sqlite3 foobar
