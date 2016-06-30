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
# the file. If you add a * to the header name, this header will be used 
# to  add an optional "id" field to the resultant JSON document.

import csv
import json
import sys
import getopt
import uuid
import os
from etllib import readEncodedVal

_verbose = False
_guessEncoding = False
_helpMessage = '''
Usage: tsvtojson [-v] [-t tsv file] [-j json file] [-o object type] [-c column headers txt file] [-u unique field] [-e encoding file] [-s threshold]

Options:
-t tsv file, --tsv=file
    Parse the given TSV file and turn it into JSON.
-j json file --json=file
    Output the named JSON file.
-c column headers file --cols=file
    Use the provided column headers to parse the TSV and to name fields in the JSON.
-o object type --object=type
    Wrap the list of objects for each row in the TSV file in the named JSON object type.    
-u unique field --unique=field
    Identifies a unique field per record in the TSV so that duplicate JSON files are not created.
-s threshold --threshold=value
    Sets the threshold for similarity for near duplicate detection. A threshold of .1 means files found to be 10% similar will be identified as duplicates.
-v, --verbose
    Work verbosely rather than silently.
-e, --encoding
    Use the provided encodings to speed up parsing
'''

def verboseLog(message):
    if _verbose:
        print >>sys.stderr, message
        
def checkFilePath(filePath, checkPath=True):
    if checkPath:
        return filePath <> None and os.path.isfile(filePath)
    else:
        return filePath <> None and not os.path.exists(filePath)

class _Usage(Exception):
    '''An error for problems with arguments on the command line.'''
    def __init__(self, msg):
        self.msg = msg

def collect(dicts):
    unique_feats = set()
    for d in dicts:
        unique_feats.update(set(d.values()))

    return unique_feats

def near_dedup_jaccard(dicts,threshold=0.1):
    all_features = collect(dicts) #set 
    # compute jaccard score
    jac_key = "jaccard_score"
    for d in dicts:
        d_feats = set()
        d_feats.update(set(d.values()))
        jaccard_score = float(len(d_feats & all_features)) / float(len(all_features))
        d[jac_key] = jaccard_score

    sorted_d = sorted(dicts, key=lambda k: k[jac_key], reverse=True) 
    dedup = list()
    pivot = sorted_d[0]
    dedup.append(pivot)
    dropped = 0

    for i in range(1, len(sorted_d)-1):
        d = sorted_d[i]
        d_feats = set()
        d_feats.update(set(d.values()))

        pivot_feats = set()
        pivot_feats.update(set(pivot.values()))

        score_diff = float(len(pivot_feats & d_feats)) / float(len(pivot_feats | d_feats)) # resemblance of pivot to d
        if score_diff < threshold:
            pivot = d
            dedup.append(pivot)
        else:
            dropped = dropped + 1
        
    verboseLog("Filtered "+str(dropped)+" near duplicates.")

    return dedup
    
    

def dedup(dicts):
    unique_sets = set(frozenset(d.items()) for d in dicts)
    unique_dicts = [dict(s) for s in unique_sets]
    return unique_dicts

def main(argv=None):
   if argv is None:
     argv = sys.argv

   try:
       try:
          opts, args = getopt.getopt(argv[1:],'hvt:j:c:o:u:e:s:',['help', 'verbose', 'tsv=','json=','cols=','object=', 'unique=', 'encoding=', 'threshold='])
       except getopt.error, msg:
         raise _Usage(msg)    
     
       if len(opts) == 0:
           raise _Usage(_helpMessage)
         
       jsonStructs = []
       cols = []
       encodings = []
       colHeaderFilePath = None
       jsonFilePath = None
       tsvFilePath = None
       objectType = None
       uniqueField = None
       encodingFilePath = None
       threshold = None
      
       for option, value in opts:
          if option in ('-h', '--help'):
             raise _Usage(_helpMessage)
          elif option in ('-t', '--tsv'):
             tsvFilePath = value
          elif option in ('-j', '--json'):
            jsonFilePath = value
          elif option in ('-c', '--cols'):
             colHeaderFilePath = value    
          elif option in ('-o', '--object'):
              objectType = value     
          elif option in ('-u', '--unique'):
              uniqueField = value   
          elif option in ('-s', '--threshold'):
              threshold = float(value)
          elif option in ('-v', '--verbose'):
             global _verbose
             _verbose = True
          elif option in ('-e', '--encoding'):
              encodingFilePath = value
      
       errorString = []
       if not checkFilePath(tsvFilePath):
          errorString.append("Error: wrong/missing tsvFilePath")            
       if not checkFilePath(jsonFilePath, False):
          errorString.append("Error: Json file path is wrong/missing or Json File already exists")
       if not checkFilePath(colHeaderFilePath):
          errorString.append("Error: wrong/missing column headers")
       if objectType == None:
           errorString.append("Error: None object type passed")
       if threshold == None:
           errorString.append("Threshold must be set to a value between 0.0 and 1.0")

       if len(errorString) > 0:
          raise _Usage(_helpMessage + '\n' + '\n'.join(errorString)) 

       _guessEncoding = encodingFilePath <> None
       if _guessEncoding:
           if checkFilePath(encodingFilePath):
               with open(encodingFilePath) as encodingFile:
                   encodings = encodingFile.read().splitlines()
                   verboseLog(encodings)
           else:
               raise _Usage("Encoding file doesn't exist")           
             
       with open(colHeaderFilePath) as headers:
           cols = headers.read().splitlines()
           verboseLog(cols)
           
       with open (tsvFilePath) as tsv:
            if uniqueField <> None:
                fieldCache = {}
                
            for line in csv.reader(tsv, dialect="excel-tab"):
                jsonStruct={}
                diff = len(cols)-len(line)
                if diff > 0:
                    verboseLog("Column Headers and Row Values Don't Match up: numCols: ["+str(len(cols))+"]: numRowValues: ["+str(len(line))+"]")
        
                for num in range(0, min(len(cols), len(line))):
                    if ":" in cols[num] and diff > 0:
                        diff = diff - 1
                        continue                    
                    
                    if line[num] <> None and line[num].lstrip() <> '':
                        val = readEncodedVal(line, num, encodings)
                    else:
                        val = ''
                    
                    jsonStruct[cols[num]] = val
                    if "*" in cols[num]:
                        jsonStruct["id"] = val    
                
                if not "id" in jsonStruct:
                    # generate an id
                    id = uuid.uuid4()
                    jsonStruct["id"] = str(id)
                            
                if uniqueField <> None:
                    if uniqueField in jsonStruct:
                        if not jsonStruct[uniqueField] in fieldCache:
                            jsonStructs.append(jsonStruct)
                            fieldCache[jsonStruct[uniqueField]] = "yes"
                        else:
                            verboseLog("Skipping adding record: ["+jsonStruct["id"]+"]: duplicate unique field: ["+jsonStruct[uniqueField]+"]")
                    else:
                        verboseLog("JSON struct with id: ["+jsonStruct["id"]+"] does not have unique field: ["+uniqueField+"]: adding it anyways.")
                else:
                    jsonStructs.append(jsonStruct)
        
       verboseLog("Deduping list of structs. Count: ["+str(len(jsonStructs))+"]")
       jsonStructs = dedup(jsonStructs)
       verboseLog("After dedup. Count: ["+str(len(jsonStructs))+"]")
       verboseLog("Near duplicates detection.")
       jsonStructs = near_dedup_jaccard(jsonStructs, threshold)
       verboseLog("After near duplicates. Count: ["+str(len(jsonStructs))+"]")
       jsonWrapper = {objectType : jsonStructs}
       outFile = open(jsonFilePath, "wb")
       verboseLog("Writing output file: ["+jsonFilePath+"]")
       json.dump(jsonWrapper, outFile, encoding="utf-8")             

   except _Usage, err:
       print >>sys.stderr, sys.argv[0].split('/')[-1] + ': ' + str(err.msg)
       return 2

if __name__ == "__main__":
    sys.exit(main())
