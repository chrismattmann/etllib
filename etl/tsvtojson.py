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
import magic
import uuid
import os


_verbose = False
_helpMessage = '''
Usage: tsvtojson [-v] [-t tsv file] [-j json file] [-o object type] [-c column headers txt file] [-u unique field]

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
-v, --verbose
    Work verbosely rather than silently.
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

def main(argv=None):
   if argv is None:
     argv = sys.argv

   try:
       try:
          opts, args = getopt.getopt(argv[1:],'hvt:j:c:o:u:',['help', 'verbose', 'tsv=','json=','cols=','object=', 'unique='])
       except getopt.error, msg:
         raise _Usage(msg)    
     
       if len(opts) == 0:
           raise _Usage(_helpMessage)
         
       jsonStructs = []
       cols = []
       colHeaderFilePath = None
       jsonFilePath = None
       tsvFilePath = None
       objectType = None
       uniqueField = None
      
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
          elif option in ('-v', '--verbose'):
             global _verbose
             _verbose = True
             
       if not checkFilePath(tsvFilePath) or not checkFilePath(jsonFilePath, False) or not checkFilePath(colHeaderFilePath) or objectType == None:
           raise _Usage(_helpMessage)      
       
       theMagic = magic.Magic(mime_encoding=True)
             
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
        
                for num in range(0, len(cols)):
                    if ":" in cols[num] and diff > 0:
                        diff = diff - 1
                        continue                    
                    
                    if line[num] <> None and line[num].lstrip() <> '':
                        encoding = None
                        try:
                            encoding = theMagic.from_buffer(line[num])
                            val = line[num].decode(encoding).encode("utf-8")
                        except magic.MagicException, err:
                            verboseLog("Error detecting encoding for row val: ["+line[num]+"]: Message: "+str(err))
                            val = line[num]                            
                    else:
                        val = line[num]
                    
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
                            verboseLog("Skipping adding record: ["+jsonStruct["id"]+"]: duplicate unique field: ["+jsonStruct[uniqueField]+"]")
                    else:
                        verboseLog("JSON struct with id: ["+jsonStruct["id"]+"] does not have unique field: ["+uniqueField+"]: adding it anyways.")
                else:
                    jsonStructs.append(jsonStruct)
        
       jsonWrapper = {objectType : jsonStructs}
       outFile = open(jsonFilePath, "wb")
       verboseLog("Writing output file: ["+jsonFilePath+"]")
       json.dump(jsonWrapper, outFile, encoding="utf-8")
       outFile.close()             

   except _Usage, err:
       print >>sys.stderr, sys.argv[0].split('/')[-1] + ': ' + str(err.msg)
       return 2

if __name__ == "__main__":
    sys.exit(main())
