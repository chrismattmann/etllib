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
"""Exact and near-duplicate detection."""

from etl.tsvtojson import dedup, near_dedup_jaccard


def record(**kw):
    base = {"title": "t", "company": "c", "location": "l", "salary": "s"}
    base.update(kw)
    return base


class TestNearDedupJaccard:
    def test_keeps_every_record_when_nothing_is_similar_enough(self):
        # Regression: the loop ran to len(sorted_d)-1, so the last record was
        # dropped even when the filter matched nothing.
        records = [
            record(title="alpha", company="one", location="x", salary="1"),
            record(title="beta", company="two", location="y", salary="2"),
            record(title="gamma", company="three", location="z", salary="3"),
        ]
        kept = near_dedup_jaccard(records, threshold=0.99)
        assert len(kept) == len(records)

    def test_last_record_survives_a_larger_input(self):
        records = [record(title="t%d" % i, company="c%d" % i,
                          location="l%d" % i, salary="s%d" % i)
                   for i in range(25)]
        kept = near_dedup_jaccard(records, threshold=0.99)
        assert len(kept) == 25

    def test_does_not_leak_the_jaccard_score_into_output(self):
        # The score is internal bookkeeping. It previously reached the output
        # documents and was indexed into Solr as a field.
        records = [record(title="alpha"), record(title="beta")]
        for kept in near_dedup_jaccard(records, threshold=0.99):
            assert "jaccard_score" not in kept

    def test_higher_threshold_keeps_at_least_as_much(self):
        # The direction is easy to read backwards: a record is discarded only
        # once it is at least `threshold` similar, so higher keeps more.
        records = [record(title="alpha", company="shared"),
                   record(title="beta", company="shared"),
                   record(title="gamma", company="shared"),
                   record(title="delta", company="shared")]
        loose = len(near_dedup_jaccard([r.copy() for r in records], threshold=0.1))
        tight = len(near_dedup_jaccard([r.copy() for r in records], threshold=0.9))
        assert tight >= loose

    def test_aggressive_threshold_collapses_similar_records(self):
        records = [record(title="alpha"), record(title="alpha"),
                   record(title="alpha"), record(title="alpha")]
        kept = near_dedup_jaccard(records, threshold=0.1)
        assert len(kept) < len(records)

    def test_empty_input_returns_empty(self):
        # Previously indexed sorted_d[0] unguarded and raised IndexError.
        assert near_dedup_jaccard([], threshold=0.8) == []

    def test_single_record_is_preserved(self):
        kept = near_dedup_jaccard([record(title="only")], threshold=0.8)
        assert len(kept) == 1
        assert kept[0]["title"] == "only"


class TestDedup:
    def test_removes_exact_duplicates(self):
        records = [record(title="alpha"), record(title="alpha"), record(title="beta")]
        assert len(dedup(records)) == 2

    def test_keeps_distinct_records(self):
        records = [record(title="alpha"), record(title="beta")]
        assert len(dedup(records)) == 2
