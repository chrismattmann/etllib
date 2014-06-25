============
ETL lib
============

This is the ETL lib package.  It provides an API
to munge and prepare JSON, TSV and other data using Apache Tika and
JSON parsing/loading for ETL via Apache OODT (or other libs)
into Apache Solr.  It also provides three command-line tools:

repackage
    Repackages an aggregate JSON file into its constituent data files (may use Tika).
poster
    Posts a JSON doc to Solr.
repackageandpost
	Combines repackage and posting to Solr without the need for an intermediate file.

For installation instructions, please see docs/INSTALL.txt.

This is licensed software; please see docs/LICENSE.txt.

For the latest news and changes, see docs/HISTORY.txt.

.. References:
