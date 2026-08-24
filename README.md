# ETLLib

[![Build](https://github.com/chrismattmann/etllib/actions/workflows/build.yml/badge.svg?branch=master)](https://github.com/chrismattmann/etllib/actions/workflows/build.yml)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](docs/LICENSE.txt)
[![Python](https://img.shields.io/badge/python-3.9%E2%80%933.13-3776AB.svg)](https://www.python.org/)
[![Website](https://img.shields.io/badge/website-chrismattmann.github.io%2Fetllib-informational.svg)](https://chrismattmann.github.io/etllib/)

**ETLLib** is a command-line toolkit and Python library for munging JSON, TSV,
and related data — using [Apache Tika](http://tika.apache.org/) where field
cleanup helps — and posting the result to
[Apache Solr](https://lucene.apache.org/solr/).

It is not a workflow engine. You run the commands from a shell, import `etl`
from Python, or call them from a
[Mnemosyne](https://github.com/chrismattmann/mnemosyne) pipeline.
[BigTranslate](https://github.com/chrismattmann/bigtranslate) does the last of
those: TSV → JSON → translate → Solr.

```
tsvtojson → repackage → poster
     TSV        split      Solr
```

It started as Python scripts on DARPA XDATA corpora (Kiva JSON dumps,
Computrabajo employment TSVs) wrapped by Apache OODT workflows. Those workflows
now run on Mnemosyne.

## Commands

Six console scripts install on your `PATH`. Each is a thin wrapper around
`etl.etllib`. Run `command -h` for the live usage string.

| Command | What it does |
|---|---|
| **tsvtojson** | TSV + column headers → one aggregate JSON file |
| **repackage** | Split an aggregate JSON into one `{id}.json` per record |
| **poster** | POST JSON documents to Solr |
| **repackageandpost** | Split and POST without writing the intermediate files |
| **translatejson** | Translate named JSON fields (Tika). Needs hirlite; see below |
| **similarity** | Jaccard similarity / clusters over a directory (Tika metadata) |

## Install

**Python 3.9–3.13.** CI covers that range. You also need the **libmagic**
shared library (`python-magic` is only the ctypes binding).

```bash
man libmagic    # should exist after install
```

macOS: `brew install libmagic`. Debian/Ubuntu: `sudo apt-get install libmagic1`.

```bash
git clone https://github.com/chrismattmann/etllib.git
cd etllib
python3 -m pip install -e .
tsvtojson -h
```

`hirlite` is declared for `translatejson`'s translation cache and often fails
to build on current Python. If `pip install -e .` dies on it, install the rest
and skip it:

```bash
python3 -m pip install setuptools iso8601 python-magic 'tika>=1.13'
python3 -m pip install --no-deps -e .
```

`translatejson` needs hirlite. For many-to-English at scale, use
[BigTranslate](https://github.com/chrismattmann/bigtranslate) / Pantogloss
instead. Tika (`tika>=1.13`) is a normal dependency; there is no separate
buildout `with-tika` extra.

## Example

A Computrabajo-style TSV, one job per row, to one JSON file per job, then Solr:

```bash
# colheaders.txt: one field name per line. "salary:" is optional if the row
# is short. "url*" is also copied to id. -s is required: higher keeps more.
tsvtojson -t data.tsv -j aggregate.json -c colheaders.txt \
          -o employmentjobs -e encoding.txt -s 0.8 -v

mkdir json && cd json
repackage -j ../aggregate.json -o employmentjobs -v

find . -name '*.json' | poster \
    -u 'http://localhost:8983/solr/jobs/update/json?commit=true' -v
```

`tsvtojson` refuses to overwrite `-j`. Encodings are tried in order, then
latin-1, with per-field UTF-8 recovery for mixed Computrabajo files. Details
and a CSV walkthrough are on the [wiki tutorial](https://github.com/chrismattmann/etllib/wiki/Simple-ETLLib-Tutorial).

## Library

```python
from etl.etllib import prepareDocs, writeDoc, recoverMisdecoded
from etl.tsvtojson import detectEncoding, near_dedup_jaccard
```

The CLIs are the supported interface. Import the same functions if you would
rather not shell out.

## Docs

- Website: <https://chrismattmann.github.io/etllib/>
- Wiki: [Home](https://github.com/chrismattmann/etllib/wiki) ·
  [Getting Started](https://github.com/chrismattmann/etllib/wiki/Getting-Started) ·
  [Tutorial](https://github.com/chrismattmann/etllib/wiki/Simple-ETLLib-Tutorial) ·
  [Commands](https://github.com/chrismattmann/etllib/wiki/Commands)

Python 2.7 / buildout notes are under
[Old](https://github.com/chrismattmann/etllib/wiki/Old). Changelog:
[docs/HISTORY.txt](docs/HISTORY.txt).

## License

Apache License 2.0. See [docs/LICENSE.txt](docs/LICENSE.txt).
