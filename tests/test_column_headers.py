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
"""Column header markers, and that they stay out of the emitted documents."""

import json

from etl.tsvtojson import main


def run(tmp_path, headers, rows, unique="alpha"):
    """Drive tsvtojson the way the command line does and return its records."""
    tsv = tmp_path / "in.tsv"
    tsv.write_text("".join("\t".join(r) + "\n" for r in rows), encoding="utf-8")
    cols = tmp_path / "cols.txt"
    cols.write_text("\n".join(headers) + "\n", encoding="utf-8")
    enc = tmp_path / "enc.txt"
    enc.write_text("utf-8\n", encoding="utf-8")
    out = tmp_path / "out.json"

    main(["tsvtojson", "-t", str(tsv), "-j", str(out), "-c", str(cols),
          "-o", "rows", "-e", str(enc), "-u", unique, "-s", "0.99"])

    parsed = json.loads(out.read_text(encoding="utf-8"))
    return parsed["rows"] if isinstance(parsed, dict) and "rows" in parsed else parsed


class TestHeaderMarkers:
    def test_optional_marker_does_not_reach_the_document(self, tmp_path):
        # Regression: ":" marks a column optional, and the loop tested for it
        # but then used the raw header as the key, so a header of
        # "phoneNumber:" produced the key "phoneNumber:". Downstream that
        # reaches Solr as a field nobody declared and no query asks for.
        records = run(tmp_path,
                      ["alpha", "phoneNumber:"],
                      [["A", "555-1234"]])
        assert records[0]["phoneNumber"] == "555-1234"
        assert "phoneNumber:" not in records[0]

    def test_id_marker_does_not_reach_the_document(self, tmp_path):
        # Same bug on the other marker: "*" names the id column.
        records = run(tmp_path, ["alpha", "sku*"], [["A", "XYZ"]])
        assert records[0]["sku"] == "XYZ"
        assert "sku*" not in records[0]

    def test_id_marker_still_sets_the_id(self, tmp_path):
        # Stripping the marker must not stop it doing its job.
        records = run(tmp_path, ["alpha", "sku*"], [["A", "XYZ"]])
        assert records[0]["id"] == "XYZ"

    def test_a_header_carrying_both_markers_is_cleaned_of_both(self, tmp_path):
        records = run(tmp_path, ["alpha", "sku*:"], [["A", "XYZ"]])
        assert records[0]["sku"] == "XYZ"
        assert records[0]["id"] == "XYZ"

    def test_plain_headers_are_untouched(self, tmp_path):
        records = run(tmp_path, ["alpha", "beta"], [["A", "B"]])
        assert records[0]["alpha"] == "A"
        assert records[0]["beta"] == "B"

    def test_optional_column_is_still_skipped_when_the_row_is_short(self, tmp_path):
        # The point of ":" is to tolerate a row that omits the column. Stripping
        # the marker from the key must not disturb that.
        records = run(tmp_path,
                      ["alpha", "beta:", "gamma", "delta"],
                      [["A", "G", "D"]])
        assert "beta" not in records[0]
        assert records[0]["alpha"] == "A"
