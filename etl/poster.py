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
# Author: mattmann
# Description: Ingests JSON docs into Solr using the JSON update handler. 
# 
# find . -name "*.json" | xargs kivaposter -u solrUrl
#
# Things to update/fix:

import os
import sys
import getopt
from etllib import prepareDocForSolr, postJsonDocToSolr

_verbose = False
_helpMessage = '''
Usage: poster [-v] [-u url] [-d directory]

Options:
-u url, --url=url
    Post to Apache Solr at the given url.
-v, --verbose
    Work verbosely rather than silently.  
-d, --directory
    reads input json files from this directory
Input: STDIN or -d
STDIN
    Line by line absolute or relative paths to JSON docs to post to Apache Solr.
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
          opts, args = getopt.getopt(argv[1:],'hvu:d:',['help', 'verbose', 'url=', 'directory='])
       except getopt.error, msg:
         raise _Usage(msg)    
     
       if len(opts) == 0:
           raise _Usage(_helpMessage)
         
       solrUrl=None
       dirFile = "" 
       for option, value in opts:
          if option in ('-h', '--help'):
             raise _Usage(_helpMessage)
          elif option in ('-u', '--url'):
             solrUrl = value
          elif option in ('-v', '--verbose'):
             global _verbose
             _verbose = True
	  elif option in ('-d', '--directory'):
             dirFile = value
       
       for filename in (os.listdir(dirFile) if dirFile else sys.stdin):
           filename = (dirFile+"\\" + filename.rstrip()) if dirFile else filename.rstrip()
           if not ".json" in filename or not os.path.exists(filename):
               continue
           verboseLog("Processing: "+filename)
           f = open(filename, 'r')
           jsonContents = f.read()
           postString = prepareDocForSolr(jsonContents)
           verboseLog(postString)
           postJsonDocToSolr(solrUrl, postString)
   except _Usage, err:
       print >>sys.stderr, sys.argv[0].split('/')[-1] + ': ' + str(err.msg)
       return 2

if __name__ == "__main__":
    sys.exit(main())

