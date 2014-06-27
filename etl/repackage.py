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
# Description: Use this script by running it against a JSON file
# that contains an outer struct of data. It will reformulate the file by removing
# the paging and other data, and just leaving the doc struct.
# Also does some cleansing of the data, using Tika and other nifty
# tools

import sys
import os
import getopt
from etllib import cleanseImage, cleanseBody, unravelStructs, formatDate, prepareDocs, writeDoc

_verbose = False
_helpMessage = '''
Usage: repackage [-v] [-j json file] [-o object type]

Options:
-j json file, --json=file
    The origin JSON object file to repackage and extract out 
    individual object types from.
-o object type --object=type
    The object type from the JSON (e.g., "journal_entries", "teams", "partners")
    to unravel from the aggregate JSON doc.
-v, --verbose
    Work verbosely rather than silently.
'''
        
def verboseLog(message):
    if _verbose:
        print >>sys.stderr, message

class _Usage(Exception):
    '''An error for problems with arguments on the command line.'''
    def __init__(self, msg):
        self.msg = msg

def main(argv=None):
   if argv is None:
     argv = sys.argv

   try:
       try:
          opts, args = getopt.getopt(argv[1:],'hvj:o:',['help', 'verbose', 'json=', 'object='])
       except getopt.error, msg:
         raise _Usage(msg)    
     
       if len(opts) == 0:
           raise _Usage(_helpMessage)
         
       jsonFile=None
       objectType=None
       
       for option, value in opts:           
          if option in ('-h', '--help'):
             raise _Usage(_helpMessage)
          elif option in ('-v', '--verbose'):
             global _verbose
             _verbose = True
          elif option in ('-j', '--json'):
             jsonFile = value
          elif option in ('-o', '--object'):
             objectType = value
             

       if not os.path.exists(jsonFile):
           print >>sys.stderr,"Path: ["+jsonFile+"] does not exist!\n"
           return 2
       
       if objectType == None:
           raise _Usage(_helpMessage)

       f = open(jsonFile, 'r')
       jsonContents = f.read()
       jsonObjects = prepareDocs(jsonContents, objectType)
       for obj in jsonObjects:
        cleanseImage(obj)
        cleanseBody(obj)
        formatDate(obj)
        unravelStructs(obj)    
        filePath = os.getcwd() + "/" + str(obj["id"])+".json"
        verboseLog("Writing json file: ["+filePath+"]")
        writeDoc(obj, filePath)
    
   except _Usage, err:
       print >>sys.stderr, sys.argv[0].split('/')[-1] + ': ' + str(err.msg)
       return 2

if __name__ == "__main__":
   sys.exit(main())
