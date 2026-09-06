"""Fast importer for the Spansh "galaxy" JSON dumps into PostgreSQL.

Layout
------
reader.py   stream a local/remote .json.gz with smart_open (+ isal) and cut it into batches
tables.py   column lists shared by the flattener and the database layer
flatten.py  turn one system JSON object into COPY text rows for every table
db.py       connections, schema, enum extension, COPY, replace-by-system merge
pipeline.py multiprocessing orchestration and progress reporting
cli.py      command line: import / sync / schema / stats
"""

__version__ = "2.0.0"
