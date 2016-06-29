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
    Takes an input JSON file and a column header file and cred file and translates from source lang to dest lang using Apache Tika.
similarity
    Computes the similarity between a directory full of files using a feature-based approach based on Jaccard's algorithm. Also clusters scores. Depends on Tika.
```

Installation
====
ETLlib can be installed with or without Tika support. Please check the relevant section below for more details.

libmagic library will need to be availabile. To test this is the check for the presence of the file command and/or the libmagic man page.

```
$ man libmagic
```

On Mac OSX, Apple has implemented their own version of the file command. However, libmagic can be installed using homebrew

```
$ brew install libmagic
```

After brew finished installing, the test for the libmagic man page should pass.

Run the following commands to install ETLLib.

```
git clone https://github.com/chrismattmann/etllib.git
cd etllib
python setup.py install
```

The `bin` directory will be populated with the various command-line tools.

License
====
This is licensed software; please see docs/LICENSE.txt.

History
====
For the latest news and changes, see docs/HISTORY.txt.
