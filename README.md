ETL lib
====

This is the ETL lib package.  It provides an API
to munge and prepare JSON, TSV and other data using Apache Tika and
JSON parsing/loading for ETL via Apache OODT (or other libs)
into Apache Solr.  It also provides the following command-line tools:

```
repackage
    Repackages an aggregate JSON file into its constituent data files (may use Tika).
poster
    Posts a JSON doc to Solr.
repackageandpost
	Combines repackage and posting to Solr without the need for an intermediate file.
tsvtojson
    Takes an input TSV file and parses it with a set of column headers and outputs a JSON file.
translatejson
    Takes an input JSON file and a column header file and cred file and translates from source lang to dest lang using Bing's API and Apache Tika.
```

Installation
====
ETLlib can be installed with or without Tika support. Please check the relevant section below for more details.

With Tika
---
The recommended way to install ETLlib is with `pip`. For Tika support you will need to install [tika-python](https://github.com/chrismattmann/tika-python) and its dependencies first. Once you have that installed, you can install ETLlib with Tika support using the following command:

```
pip install git+https://github.com/chrismattmann/etllib.git[Tika]
```

Without Tika
---
The recommended way to install ETLlib is with `pip`. To install without Tika support simply run the following command.

```
pip install git+https://github.com/chrismattmann/etllib.git
```

License
====
This is licensed software; please see docs/LICENSE.txt.

History
====
For the latest news and changes, see docs/HISTORY.txt.
