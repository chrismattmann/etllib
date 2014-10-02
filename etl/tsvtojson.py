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
import magic
import uuid
import os
  



class tsvtojson:
    class _Usage(Exception):
        msg = '''An error for problems with arguments on the command line.'''
        def __init__(self, msg):
            self.msg = msg
            
    def __init__(self,tsvFile = None,jsonFile = None,objectType = None,colFile = None,uniqueField = None,encFile = None,verbose = False):
       self._verbose = verbose
       self._guessEncoding = False
       self._theMagic = magic.Magic(mime_encoding=True)
       self._helpMessage = '''
        Usage: tsvtojson(tsvfile,jsonfile,objecttype,columnheaderstxtfile,uniquefield,encoding file)
        
        Mandotory for correct fuctionality
        tsvfile
            Parse the given TSV file and turn it into JSON.
        jsonfile
            Output the named JSON file.
        columnheaderstxtfile
            Use the provided column headers to parse the TSV and to name fields in the JSON.
        objecttype
            Wrap the list of objects for each row in the TSV file in the named JSON object type.    
        uniquefield 
            Identifies a unique field per record in the TSV so that duplicate JSON files are not created.
        
        Options:
        verbose
            Work verbosely rather than silently.
        encoding
            Use the provided encodings to speed up parsing
        '''
    
       try:             
           jsonStructs = []
           cols = []
           encodings = []
           self.colHeaderFilePath = colFile
           self.jsonFilePath = jsonFile
           self.tsvFilePath = tsvFile
           self.objectType = objectType
           self.uniqueField = uniqueField
           self.encodingFilePath = encFile
          
                 
           if not self.checkFilePath(self.tsvFilePath) or not self.checkFilePath(self.jsonFilePath, False) or not self.checkFilePath(self.colHeaderFilePath) or objectType == None:
               raise self._Usage(self._helpMessage)      
    
           _guessEncoding = self.encodingFilePath <> None
           if _guessEncoding:
               if self.checkFilePath(self.encodingFilePath):
                   with open(self.encodingFilePath) as encodingFile:
                       encodings = encodingFile.read().splitlines()
                       self.verboseLog(encodings)
               else:
                   raise self._Usage("Encoding file doesn't exist")           
                 
           with open(self.colHeaderFilePath) as headers:
               cols = headers.read().splitlines()
               self.verboseLog(cols)
               
           with open (self.tsvFilePath) as tsv:
                if uniqueField <> None:
                    fieldCache = {}
                    
                for line in csv.reader(tsv, dialect="excel-tab"):
                    jsonStruct={}
                    diff = len(cols)-len(line)
                    if diff > 0:
                        self.verboseLog("Column Headers and Row Values Don't Match up: numCols: ["+str(len(cols))+"]: numRowValues: ["+str(len(line))+"]")
            
                    for num in range(0, min(len(cols), len(line))):
                        if ":" in cols[num] and diff > 0:
                            diff = diff - 1
                            continue                    
                        
                        if line[num] <> None and line[num].lstrip() <> '':
                            if _guessEncoding:
                                for encoding in encodings:
                                    try:
                                        val = line[num].decode(encoding).encode("utf-8")
                                    except UnicodeDecodeError:
                                        if encoding <> encodings[-1]:
                                            continue
                                        val = self.convertToUTF8(line[num])
                                    else:
                                        break
                            else:
                                val = self.convertToUTF8(line[num])
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
                                jsonStructs.append(jsonStruct)
                            else:
                                self.verboseLog("Skipping adding record: ["+jsonStruct["id"]+"]: duplicate unique field: ["+jsonStruct[uniqueField]+"]")
                        else:
                            self.verboseLog("JSON struct with id: ["+jsonStruct["id"]+"] does not have unique field: ["+uniqueField+"]: adding it anyways.")
                    else:
                        jsonStructs.append(jsonStruct)
            
           jsonWrapper = {objectType : jsonStructs}
           outFile = open(self.jsonFilePath, "wb")
           self.verboseLog("Writing output file: ["+self.jsonFilePath+"]")
           json.dump(jsonWrapper, outFile, encoding="utf-8")
           outFile.close()             
    
       except self._Usage, err:
           print >>sys.stderr, sys.argv[0].split('/')[-1] + ': ' + str(err.msg)
           return 2
       
    def checkFilePath(self,filePath, checkPath=True):
        if checkPath:
            return filePath <> None and os.path.isfile(filePath)
        else:
            return filePath <> None and not os.path.exists(filePath)
    
    def verboseLog(self,message):
        if self._verbose:
            print >>sys.stderr, message

    def convertToUTF8(self,src):
        try:
            encoding = self._theMagic.from_buffer(src)
            val = src.decode(encoding).encode("utf-8")
        except magic.MagicException, err:
            self.verboseLog("Error detecting encoding for row val: ["+src+"]: Message: "+str(err))
            val = src
        except LookupError, err:
            self.verboseLog("unknown encoding: binary:"+src+":Message:"+str(err))
            val = src
        finally:
            return val


