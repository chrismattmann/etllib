#!/usr/bin/env python
# encoding: utf-8
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
# 
# $Id$
#
# Author: mattmann
# Description: Use this script by running it against a TSV file that 
# you would like to transform into a JSON file. You need to pass this 
# script a text file representing the column headers in the TSV file
# that you are transforming. Each line in the column headers text file
# is simply the header name. If you add a ":" postfix to the header name,
# the header is considered optional and will only be read if present in
# the file.

import csv
import json

jsonStructs = []
cols = []
with open("colheaders.txt") as headers:
    cols = headers.read().splitlines()
    print cols

with open ("computrabajo-ve-20121108.tsv") as tsv:
    for line in csv.reader(tsv, dialect="excel-tab"):
        jsonStruct={}
        diff = len(cols)-len(line)

        for num in range(0, len(cols)-1):
            if ":" in cols[num] and diff > 0:
                diff = diff - 1
                continue

            jsonStruct[cols[num]] = line[num]
        jsonStructs.append(jsonStruct)

outFile = open("foo.json", "wb")
json.dump(jsonStructs, outFile, encoding="latin-1")

