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
# Description: A suite of functions for munging data in preparation
# for loading into Solr using Tika and other goodies (JSON parsing, etc.)

import urllib2
urllib2.build_opener(urllib2.HTTPHandler(debuglevel=1))
import json
import os
import operator

try:
    import tika
    from tika import parser
    tika.initVM()
    tika_support = True
except ImportError:
    tika_support = False

import magic
_theMagic = magic.Magic(mime_encoding=True)

def prepareDocs(jsondata, objectType):
    jsondoc = json.loads(jsondata)
    return jsondoc[objectType]

def writeDoc(theDoc, filePath):    
    try:
        f = open(filePath, 'w')
        try:
            f.write(json.dumps(theDoc))
        finally:
            f.close()
    except IOError:
        pass

def cleanseImage(theDoc):
    if "image" in theDoc:
        theDoc["image_id"] = theDoc["image"]["id"]
        theDoc["image_template_id"] = theDoc["image"]["template_id"]
        theDoc.pop("image", None)
        
def cleanseBody(theDoc):
    if not tika_support:
        print (
            "cleanseBody requires Tika to be installed."
            "Please check the documentation on how to install "
            "ETLlib with Tika support."
        )
        raise RuntimeError("Tika support not installed.")

    if "body" in theDoc:
        content = "<html>".encode('utf-8')+theDoc["body"].encode('utf-8')+"</html>".encode('utf-8')
        parsed = parser.from_buffer(content)
        for key,val in parsed["metadata"].iteritems():
            if key not in theDoc:
                theDoc[key] = val
            else:
                theDoc["tika_"+key] = val
        theDoc["body"] = parsed["content"]


def readEncodedVal(line, colnum, encodings=None):
    val = None
    if encodings != None:
        for encoding in encodings:
            try:
                val = line[colnum].decode(encoding).encode("utf-8")
            except UnicodeDecodeError:
                if encoding != encodings[-1]:
                    continue
                val = convertToUTF8(line[colnum])
            else:
                break
    else:
        val = convertToUTF8(line[colnum])
    return val


def convertToUTF8(src):
    try:
        encoding = _theMagic.from_buffer(src)
        val = src.decode(encoding).encode("utf-8")
    except magic.MagicException, err:
        verboseLog("Error detecting encoding for row val: ["+src+"]: Message: "+str(err))
        val = src
    except LookupError, err:
        verboseLog("unknown encoding: binary:"+src+":Message:"+str(err))
        val = src
    finally:
        return val


def unravelStructs(theDoc):
    if "countries" in theDoc:
        countriesJson = theDoc["countries"]
        for country in countriesJson:
            if "region" in country:
                _createOrAppendToList(theDoc, "countries_region", country["region"]) 
            if "name" in country:
                _createOrAppendToList(theDoc, "countries_name", country["name"])
            if "iso_code" in country:
                _createOrAppendToList(theDoc, "countries_iso_code", country["iso_code"])
            if "location" in country and "geo" in country["location"]:
                if "level" in country["location"]["geo"]:
                    _createOrAppendToList(theDoc, "countries_location_geo_level", country["location"]["geo"]["level"])
                if "pairs" in country["location"]["geo"]:
                    _createOrAppendToList(theDoc, "countries_location_geo_pairs", country["location"]["geo"]["pairs"])
                if "type" in country["location"]["geo"]:
                    _createOrAppendToList(theDoc, "countries_location_geo_type", country["location"]["geo"]["type"])
        theDoc.pop("countries", None)
        
def requiresDateFormating(dateString):
    if 'T' not in dateString:
        if  re.search('^\d{4}-\d{2}-\d{2}$', dateString) == None:
            raise RuntimeError("Incorrect DateTime format. Check solr DateField.")
        return True
    if  re.search('^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,3})?Z$', dateString) == None:
        raise RuntimeError("Incorrect DateTime format. Check solr DateField.")
    return False

def formatDate(theDoc):
    for key in theDoc.iterkeys():
        if "date" in key.lower():
            value = theDoc[key]
            if requiresDateFormating(value):
                theDoc[key] = value+"T00:00:00.000Z"
        
def postJsonDocToSolr(solrUrl, data):
    print "POST "+solrUrl
    req = urllib2.Request(solrUrl, data, {'Content-Type': 'application/json'})
    try:
        f = urllib2.urlopen(req)
        print f.read()
    except urllib2.HTTPError, (err):
        print "HTTP error(%s)" % (err)


def prepareDocForSolr(jsondata, unmarshall=True, encoding='utf-8'):
    jsondoc = json.loads(jsondata, encoding=encoding) if unmarshall else jsondata
    if "boost" in jsondoc:
        boost = jsondoc["boost"]
    else:
        boost = 1.0
    jsonwrapper = {"add": {"boost": boost, "doc": jsondoc}}
    return json.dumps(jsonwrapper, encoding=encoding)

def prepareDocsForSolr(jsondata, unmarshall=True, encoding='utf-8'):
    jsondocs = json.loads(jsondata, encoding=encoding) if unmarshall else jsondata
    return json.dumps(jsondocs, encoding=encoding)



def compareKeySimilarity ( fileDir, encoding = 'utf-8') :

    union_feature_names = set()
    file_parsed_data = {}
    resemblance_scores = {}

    for filename in fileDir:
        parsedData = parser.from_file(filename)
        file_parsed_data[filename] = parsedData["metadata"]
        union_feature_names = union_feature_names | set(parsedData["metadata"].keys())

    total_num_features = len(union_feature_names)

    for filename in file_parsed_data.keys():
        overlap = {}
        overlap = set(file_parsed_data[filename].keys()) & set(union_feature_names) 
        resemblance_scores[filename] = float(len(overlap))/total_num_features

    sorted_resemblance_scores = sorted(resemblance_scores.items(), key=operator.itemgetter(1), reverse=True)

    return sorted_resemblance_scores, file_parsed_data

def compareValueSimilarity ( fileDir, encoding = 'utf-8') :
    union_feature_names = set()
    file_parsed_data = {}
    resemblance_scores = {}

    for filename in fileDir:
        file_parsed = []
        # first compute the union of all features
        parsedData = parser.from_file(filename)
        #get key : value of metadata
        for key in parsedData["metadata"].keys() :
            file_parsed.append(str(key.strip() + ": " + parsedData["metadata"].get(key).strip()))


        file_parsed_data[filename] = set(file_parsed)
        union_feature_names = union_feature_names | set(file_parsed_data[filename])

    total_num_features = len(union_feature_names)
    
    for filename in file_parsed_data.keys():
        overlap = {}
        overlap = file_parsed_data[filename] & set(union_feature_names) 
        resemblance_scores[filename] = float(len(overlap))/total_num_features

    sorted_resemblance_scores = sorted(resemblance_scores.items(), key=operator.itemgetter(1), reverse=True)
    return sorted_resemblance_scores, file_parsed_data

def convertKeyUnicode( fileDict, key = None) :
    fileUTFDict = {}
    for key in fileDict.keys():
        if isinstance(key, unicode) :
            key = str(key).strip(" ")
        value = fileDict.get(key)
        if isinstance(value, unicode) :
            value = str(value).strip(" ")
        fileUTFDict[key] = value
        
    return str(fileUTFDict)


def convertValueUnicode( fileDict ) :
    fileUTFDict = []
    for record in fileDict:
        if isinstance(record, unicode) :
            record = str(record).strip(" ")
        fileUTFDict.append(record)
        
    return str(fileUTFDict)
    
def _createOrAppendToList(doc, key, val):
    if key in doc:
        doc[key].append(val)
    else:
        doc[key] = [val]
