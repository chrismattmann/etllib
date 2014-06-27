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
# Description: this is a python script that combines the repackage
# functionality for JSON entries, skips writing the intermediate file
# processes and cleanses the entries from the file and posts them
# as documents to solr.
# 
# find . -name "*.json" | xargs /path/to/repackageandposter.py -u solrUrl
#
# Things to update/fix:


from etllib import prepareDocs, cleanseBody, cleanseImage, unravelStructs, formatDate, prepareDocForSolr, postJsonDocToSolr
import getopt
import sys
import os


_verbose = False
_helpMessage = '''
Usage: repackageandpost [-v] [-u url] [-o object type]

Options:
-u url, --url=url
    Post to Apache Solr at the given url.

-o object type --object=type
    The object type from the JSON file (e.g., "journal_entries", "teams", "partners")
    to unravel from the aggregate JSON doc.
    
-v, --verbose
    Work verbosely rather than silently.
    
Input:
STDIN
    Line by line absolute or relative paths to JSON docs that it will transform and
    post to Apache Solr.
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
          opts, args = getopt.getopt(argv[1:],'hvu:o:',['help', 'verbose', 'url=', 'object='])
       except getopt.error, msg:
         raise _Usage(msg)    
     
       if len(opts) == 0:
           raise _Usage(_helpMessage)
         
       solrUrl=None
       objectType=None
       
       for option, value in opts:           
          if option in ('-h', '--help'):
             raise _Usage(_helpMessage)
          elif option in ('-v', '--verbose'):
             global _verbose
             _verbose = True
          elif option in ('-u', '--url'):
             solrUrl = value
          elif option in ('-o', '--object'):
             objectType = value
             

       if solrUrl == None or objectType == None:
           raise _Usage(_helpMessage)

       for filename in sys.stdin:
           filename = filename.rstrip()
           if not ".json" in filename or not os.path.exists(filename):
               continue
           verboseLog("Processing: "+filename)
           f = open(filename, 'r')
           jsonContents = f.read()
           jsonObjs = prepareDocs(jsonContents, objectType)
           for obj in jsonObjs:
               cleanseImage(obj)
               cleanseBody(obj)
               formatDate(obj)
               unravelStructs(obj)    
               postString = prepareDocForSolr(obj, False)
               verboseLog(postString)
               postJsonDocToSolr(solrUrl, postString)

    
   except _Usage, err:
       print >>sys.stderr, sys.argv[0].split('/')[-1] + ': ' + str(err.msg)
       return 2

if __name__ == "__main__":
   sys.exit(main())


