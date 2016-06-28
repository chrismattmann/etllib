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
# Description: TBD

import json
import sys
import getopt
from tika import translate
import hirlite

_verbose = False
_helpMessage = '''
Usage: translate [-v] [-c column headers file] [-i input json file] [-j output json file] [-p cred file] [-r translation cache file] [-f from] [-t to]

Options:
-i input json file --injson=file
    The input named JSON file.
-j json file --json=file
    Output the named JSON file.
-c column headers file --cols=file
    Use the provided column headers to parse the TSV and to name fields in the JSON.
-f from language --from=2 letter language code
    The 2 letter code of the language to translate from.
-t to language --to=2 letter language code
    The 2 letter code of the language to translate to.
-r path  --rlite=path to rlite file
    The path to the rlite-py translation cache for efficiency and to avoid lookups. 
-v, --verbose
    Work verbosely rather than silently.
'''

_rlite = None

def initRLite(the_path='./translation.rdb'):
    global _rlite
    _rlite = hirlite.Rlite(encoding='utf8', path=the_path)

def verboseLog(message):
    if _verbose:
        print >>sys.stderr, message

def cacheTranslation(original, translated):
    global _rlite
    _rlite.command('set', original.encode('utf-8', 'ignore'), translated.encode('utf-8', 'ignore'));

def translateFromCache(original):
    return _rlite.command('get', original.encode('utf-8', 'ignore'))

class _Usage(Exception):
    '''An error for problems with arguments on the command line.'''
    def __init__(self, msg):
        self.msg = msg

def main(argv=None):
   if argv is None:
     argv = sys.argv

   try:
       try:
          opts, args = getopt.getopt(argv[1:],'hvj:c:f:t:i:r:',['help', 'verbose','json=','cols=','from=','to=','injson=', 'rlite='])
       except getopt.error, msg:
         raise _Usage(msg)    
     
       if len(opts) == 0:
           raise _Usage(_helpMessage)
         
       cols = []
       colHeaderFilePath = None
       outputJsonFilePath = None
       fromLang = None
       toLang = None
       inputJsonFilePath = None
       rlitePath = None
      
       for option, value in opts:
          if option in ('-h', '--help'):
             raise _Usage(_helpMessage)
          elif option in ('-i', '--injson'):
            inputJsonFilePath = value
          elif option in ('-j', '--json'):
            outputJsonFilePath = value
          elif option in ('-r', '--rlite'):
            rlitePath = value
          elif option in ('-c', '--cols'):
             colHeaderFilePath = value    
          elif option in ('-f', '--from'):
              fromLang = value
          elif option in ('-t', '--to'):
              toLang = value
          elif option in ('-v', '--verbose'):
             global _verbose
             _verbose = True
             
       if inputJsonFilePath == None or outputJsonFilePath == None or colHeaderFilePath == None or fromLang == None or toLang == None or rlitePath == None:
           raise _Usage(_helpMessage)      
    
       initRLite(rlitePath)
   
       with open(colHeaderFilePath) as headers:
           cols = headers.read().splitlines()
           verboseLog(cols)

       with open(inputJsonFilePath) as jsonFile:
           jsonData = jsonFile.read()
           jsonStruct = json.loads(jsonData)
              
       for col in cols:
           if col in jsonStruct:
               translated = translateFromCache(jsonStruct[col])
               if translated == None:
                   translated = translate.from_buffer(jsonStruct[col], fromLang, toLang)
                   verboseLog("translating: file: ["+inputJsonFilePath+"]: field: ["+col+"]: orig: ["+jsonStruct[col]+"]: to translated: ["+translated+"]")
                   cacheTranslation(jsonStruct[col], translated)
               jsonStruct[col] = translated
               
           else:
               print "column ["+col+"] not present in json file: ["+inputJsonFilePath+"]"
    
       outFile = open(outputJsonFilePath, "wb")
       verboseLog("Writing output file: ["+outputJsonFilePath+"]")
       json.dump(jsonStruct, outFile, encoding="utf-8")             

   except _Usage, err:
       print >>sys.stderr, sys.argv[0].split('/')[-1] + ': ' + str(err.msg)
       return 2


if __name__ == "__main__":
    sys.exit(main())
