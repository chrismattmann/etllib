# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Preparing and posting documents to Solr.

Every check here corresponds to a Python 3 removal that made posting fail
outright, so no document reached Solr at all.
"""

import importlib
import json

import pytest

from etl import etllib
from etl.etllib import prepareDocForSolr, prepareDocsForSolr


class TestPrepareDocForSolr:
    def test_accepts_a_str_payload(self):
        out = json.loads(prepareDocForSolr('{"title": "Analista"}'))
        assert out["add"]["doc"]["title"] == "Analista"

    def test_accepts_a_bytes_payload(self):
        # json.loads lost its encoding keyword in Python 3.9; bytes input has
        # to be decoded before parsing rather than passed through.
        out = json.loads(prepareDocForSolr('{"title": "Analista"}'.encode("utf-8")))
        assert out["add"]["doc"]["title"] == "Analista"

    def test_preserves_accented_values(self):
        payload = json.dumps({"location": "República Dominicana"})
        out = json.loads(prepareDocForSolr(payload))
        assert out["add"]["doc"]["location"] == "República Dominicana"

    def test_decodes_bytes_with_the_requested_encoding(self):
        payload = json.dumps({"city": "México"}, ensure_ascii=False).encode("latin-1")
        out = json.loads(prepareDocForSolr(payload, encoding="latin-1"))
        assert out["add"]["doc"]["city"] == "México"

    def test_default_boost_is_applied(self):
        out = json.loads(prepareDocForSolr('{"title": "x"}'))
        assert out["add"]["boost"] == 1.0

    def test_explicit_boost_is_carried_through(self):
        out = json.loads(prepareDocForSolr('{"title": "x", "boost": 2.5}'))
        assert out["add"]["boost"] == 2.5

    def test_unmarshall_false_passes_the_object_through(self):
        out = json.loads(prepareDocForSolr({"title": "x"}, unmarshall=False))
        assert out["add"]["doc"]["title"] == "x"


class TestPrepareDocsForSolr:
    def test_round_trips_a_list_of_documents(self):
        out = json.loads(prepareDocsForSolr('[{"a": 1}, {"a": 2}]'))
        assert out == [{"a": 1}, {"a": 2}]

    def test_accepts_bytes(self):
        out = json.loads(prepareDocsForSolr(b'[{"a": 1}]'))
        assert out == [{"a": 1}]


class TestPostJsonDocToSolr:
    """urllib requires a bytes body on Python 3; str raised TypeError."""

    def test_encodes_a_str_body_to_bytes(self, monkeypatch):
        captured = {}

        class FakeResponse:
            def read(self):
                return b'{"responseHeader":{"status":0}}'

        def fake_request(url, data, headers):
            captured["url"] = url
            captured["data"] = data
            captured["headers"] = headers
            return "request"

        monkeypatch.setattr(etllib.urllib2, "Request", fake_request)
        monkeypatch.setattr(etllib.urllib2, "urlopen", lambda req: FakeResponse())

        etllib.postJsonDocToSolr("http://localhost:8080/solr/x/update", '{"a": "b"}')

        assert isinstance(captured["data"], bytes)
        assert captured["data"] == b'{"a": "b"}'

    def test_leaves_a_bytes_body_alone(self, monkeypatch):
        captured = {}

        class FakeResponse:
            def read(self):
                return b"ok"

        monkeypatch.setattr(
            etllib.urllib2, "Request",
            lambda url, data, headers: captured.update(data=data) or "request")
        monkeypatch.setattr(etllib.urllib2, "urlopen", lambda req: FakeResponse())

        etllib.postJsonDocToSolr("http://localhost/solr", b'{"a": "b"}')
        assert captured["data"] == b'{"a": "b"}'

    def test_declares_a_utf8_charset(self, monkeypatch):
        captured = {}

        class FakeResponse:
            def read(self):
                return b"ok"

        monkeypatch.setattr(
            etllib.urllib2, "Request",
            lambda url, data, headers: captured.update(headers=headers) or "request")
        monkeypatch.setattr(etllib.urllib2, "urlopen", lambda req: FakeResponse())

        etllib.postJsonDocToSolr("http://localhost/solr", '{"city": "México"}')
        assert "utf-8" in captured["headers"]["Content-Type"].lower()

    def test_accented_body_survives_encoding(self, monkeypatch):
        captured = {}

        class FakeResponse:
            def read(self):
                return b"ok"

        monkeypatch.setattr(
            etllib.urllib2, "Request",
            lambda url, data, headers: captured.update(data=data) or "request")
        monkeypatch.setattr(etllib.urllib2, "urlopen", lambda req: FakeResponse())

        body = json.dumps({"city": "México"}, ensure_ascii=False)
        etllib.postJsonDocToSolr("http://localhost/solr", body)
        assert json.loads(captured["data"].decode("utf-8"))["city"] == "México"


@pytest.mark.parametrize(
    "module",
    [
        "etl.etllib",
        "etl.poster",
        "etl.repackage",
        "etl.repackageandpost",
        "etl.similarity",
        "etl.tsvtojson",
        pytest.param(
            "etl.translatejson",
            marks=pytest.mark.xfail(
                raises=ModuleNotFoundError,
                reason="imports hirlite, which no longer builds on any current "
                       "Python; superseded by BigTranslate's Pantogloss step",
            ),
        ),
    ],
)
def test_module_imports(module):
    """poster.py mixed a tab with spaces and raised TabError before parsing."""
    assert importlib.import_module(module) is not None
