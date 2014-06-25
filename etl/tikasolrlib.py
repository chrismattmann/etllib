#!/usr/bin/env python
# encoding: utf-8
#
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
# Description: A suite of functions for munging the Kiva data in preparation
# for loading into Solr using Tika and other goodies (JSON parsing, etc.)

import tika
from tika import parser
tika.initVM()
import urllib2
urllib2.build_opener(urllib2.HTTPHandler(debuglevel=1))
import json

def prepareDocs(jsondata, objectType):
    jsondoc = json.loads(jsondata)
    return jsondoc[objectType]

def writeDoc(kivaDoc, filePath):    
    try:
        f = open(filePath, 'w')
        try:
            f.write(json.dumps(kivaDoc))
        finally:
            f.close()
    except IOError:
        pass

def cleanseImage(kivaDoc):
    if "image" in kivaDoc:
        kivaDoc["image_id"] = kivaDoc["image"]["id"]
        kivaDoc["image_template_id"] = kivaDoc["image"]["template_id"]
        kivaDoc.pop("image", None)
        
def cleanseBody(kivaDoc):
    if "body" in kivaDoc:
        content = "<html>".encode('utf-8')+kivaDoc["body"].encode('utf-8')+"</html>".encode('utf-8')
        parsed = parser.from_buffer(content)
        for key,val in parsed["metadata"].iteritems():
            if key not in kivaDoc:
                kivaDoc[key] = val
            else:
                kivaDoc["tika_"+key] = val
        kivaDoc["body"] = parsed["content"]
        

def unravelStructs(kivaDoc):
    if "countries" in kivaDoc:
        countriesJson = kivaDoc["countries"]
        for country in countriesJson:
            if "region" in country:
                _createOrAppendToList(kivaDoc, "countries_region", country["region"]) 
            if "name" in country:
                _createOrAppendToList(kivaDoc, "countries_name", country["name"])
            if "iso_code" in country:
                _createOrAppendToList(kivaDoc, "countries_iso_code", country["iso_code"])
            if "location" in country and "geo" in country["location"]:
                if "level" in country["location"]["geo"]:
                    _createOrAppendToList(kivaDoc, "countries_location_geo_level", country["location"]["geo"]["level"])
                if "pairs" in country["location"]["geo"]:
                    _createOrAppendToList(kivaDoc, "countries_location_geo_pairs", country["location"]["geo"]["pairs"])
                if "type" in country["location"]["geo"]:
                    _createOrAppendToList(kivaDoc, "countries_location_geo_type", country["location"]["geo"]["type"])
        kivaDoc.pop("countries", None)
        

def postJsonDocToSolr(solrUrl, data):
    print "POST "+solrUrl
    #print "posting: "+data
    req = urllib2.Request(solrUrl, data, {'Content-Type': 'application/json'})
    try:
        f = urllib2.urlopen(req)
        print f.read()
    except urllib2.HTTPError, (err):
        print "HTTP error(%s)" % (err)


def prepareDocForSolr(jsondata, unmarshall=True):
    jsondoc = json.loads(jsondata) if unmarshall else jsondata
    if "boost" in jsondoc:
        boost = jsondoc["boost"]
    else:
        boost = 1.0
    jsonwrapper = {"add": {"boost": boost, "doc": jsondoc}}
    return json.dumps(jsonwrapper)

def _createOrAppendToList(doc, key, val):
    if key in doc:
        doc[key].append(val)
    else:
        doc[key] = [val]
