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
For Tika support you will need to install [tika-python](https://github.com/chrismattmann/tika-python) and its dependencies first. Once you have that installed, you can install ETLlib with Tika support using the following commands:

```
git clone https://github.com/chrismattmann/etllib.git
cd etllib
python bootstrap.py
bin/buildout install with-tika
```

The `bin` directory will be populated with the various command-line tools.


Without Tika
---
To install without Tika support simply run the following commands:

```
python bootstrap.py
bin/buildout install without-tika
```

The `bin` directory will be populated with the various command-line tools.


License
====
This is licensed software; please see docs/LICENSE.txt.

History
====
For the latest news and changes, see docs/HISTORY.txt.
