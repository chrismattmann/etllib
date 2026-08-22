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
"""Encoding handling for TSV input.

These cover the Python 2 to Python 3 gap: under Python 2 open() returned bytes
and every value was decoded individually, so a file could mix encodings across
fields. Under Python 3 decoding happens at read time, which has to be handled
explicitly or mixed files are either rejected outright or silently mangled.
"""

import pytest

from etl.etllib import recoverMisdecoded
from etl.tsvtojson import detectEncoding


class TestDetectEncoding:
    def test_prefers_the_first_declared_encoding_that_works(self, tmp_path):
        f = tmp_path / "utf8.tsv"
        f.write_bytes("Mexico\tMéxico\n".encode("utf-8"))
        assert detectEncoding(str(f), ["utf-8", "latin-1"]) == "utf-8"

    def test_falls_through_to_a_later_encoding(self, tmp_path):
        # 0xe9 is a valid latin-1 'e-acute' and an invalid UTF-8 continuation.
        f = tmp_path / "latin1.tsv"
        f.write_bytes(b"M\xe9xico\tSantiago\n")
        assert detectEncoding(str(f), ["utf-8", "latin-1"]) == "latin-1"

    def test_falls_back_to_latin1_when_nothing_declared_fits(self, tmp_path):
        f = tmp_path / "odd.tsv"
        f.write_bytes(b"M\xe9xico\n")
        # latin-1 maps every byte, so a usable encoding always comes back.
        assert detectEncoding(str(f), ["utf-8", "us-ascii"]) == "latin-1"

    def test_defaults_to_utf8_when_no_list_is_given(self, tmp_path):
        f = tmp_path / "plain.tsv"
        f.write_bytes(b"plain ascii\n")
        assert detectEncoding(str(f), None) == "utf-8"

    def test_ignores_an_unknown_codec_name(self, tmp_path):
        f = tmp_path / "x.tsv"
        f.write_bytes(b"hello\n")
        assert detectEncoding(str(f), ["definitely-not-a-codec", "utf-8"]) == "utf-8"

    def test_whole_file_is_checked_not_just_the_first_line(self, tmp_path):
        # A file that only goes wrong late must not be reported as utf-8.
        f = tmp_path / "late.tsv"
        f.write_bytes(b"clean ascii line\n" * 500 + b"M\xe9xico\n")
        assert detectEncoding(str(f), ["utf-8", "latin-1"]) == "latin-1"


class TestRecoverMisdecoded:
    def test_repairs_utf8_bytes_read_as_latin1(self):
        # What the 2012 scrape produced: UTF-8 bytes inside an otherwise
        # latin-1 file, so reading as latin-1 yields mojibake.
        mojibake = "Miguel A. Mu\xc3\xb1oz"
        assert recoverMisdecoded(mojibake) == "Miguel A. Muñoz"

    def test_leaves_genuine_latin1_alone(self):
        # 0xe9 read as latin-1 is already correct and must not be touched.
        assert recoverMisdecoded("México") == "México"

    @pytest.mark.parametrize(
        "mojibake,expected",
        [
            ("Pr\xc3\xa1cticas", "Prácticas"),
            ("Rep\xc3\xbablica", "República"),
            ("Se\xc3\xb1or", "Señor"),
        ],
    )
    def test_repairs_common_corpus_values(self, mojibake, expected):
        assert recoverMisdecoded(mojibake) == expected

    def test_passes_through_plain_ascii(self):
        assert recoverMisdecoded("Customer Service Agents") == "Customer Service Agents"

    def test_passes_through_non_strings(self):
        assert recoverMisdecoded(None) is None
        assert recoverMisdecoded(17) == 17

    def test_leaves_text_that_cannot_round_trip(self):
        # Not encodable as latin-1, so there is nothing to reinterpret.
        assert recoverMisdecoded("日本語") == "日本語"

    def test_is_idempotent(self):
        once = recoverMisdecoded("Mu\xc3\xb1oz")
        assert recoverMisdecoded(once) == once
