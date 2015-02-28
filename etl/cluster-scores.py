#!/usr/bin/env python2.7
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
# 
#    http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# 
#

import json
import getopt
import sys
import os


_verbose = False
_helpMessage = '''

Usage: similarity [-f --directory] [-t --threshold] [-o --output] [-v --verbose] [-h --help]
Options:


-f, --directory [path to directory]
    read similarity-scores.txt file from this directory
-t, --threshold [value of threshold]
    set threshold for cluster similarity
-o, --output
    the directory of output files
-v, --verbose
    Work verbosely rather than silently.
-h --help
    show help on the screen 
'''

def verboseLog(message):
    if _verbose:
        print >>sys.stderr, message

class _Usage(Exception):
    ''' an error for arguments '''

    def __init__(self, msg):
        self.msg = msg

def main(argv = None):
    if argv is None:
        argv = sys.argv

    try:
        try:
            opts, args = getopt.getopt(argv[1:], 'f:t:o:vh', ['directory=', 'threshold=', 'output=', 'verbose', 'help'])
        except getopt.error, msg:
            raise _Usage(msg)

        if len(opts) ==0:
            raise _Usage(_helpMessage)

        dirFile = ""
        threshold = None
        output_dir = ""
        for option, value in opts:

            if option in ('-f', '--directory') :
                dirFile = value
            elif option in ('-t', '--threshold') :
                threshold = float(value)
            elif option in ('-o', '--output') :
                output_dir = value
            elif option in ('-h', '--help') :
                raise _Usage(_helpMessage)
            elif option in ('-v', '--verbose') :
                global _verbose
                _verbose = True


        if not os.path.isfile(dirFile) :
            raise _Usage("file does not exists!")

        else :
            with open(dirFile, 'r') as f:
                prior = None
                clusters = []
                clusterCount = 0
                cluster = {"name":"cluster"+str(clusterCount)}
                clusterData = []
                for line in f:
                    featureDataList = line.split(",",2) # file name,score, metadata
                    if len(featureDataList) != 3:
                        continue

                    if prior != None:
                        diff = prior-float(featureDataList[1])
                    else:
                        diff = -1.0

                    # cleanse the \n
                    featureDataList[1] = featureDataList[1].strip()
                    featureData = {"name":featureDataList[0], "score":float(featureDataList[1]), "metadata" : featureDataList[2]}

                    if diff > threshold:
                        cluster["children"] = clusterData
                        clusters.append(cluster)
                        clusterCount = clusterCount + 1
                        cluster = {"name":"cluster"+str(clusterCount)}
                        clusterData = []
                        clusterData.append(featureData)
                        prior = float(featureDataList[1])
                    else:
                        clusterData.append(featureData)
                        prior = float(featureDataList[1])
                        #print featureDataList[2]

                #add the last cluster into clusters
                cluster["children"] = clusterData
                clusters.append(cluster)
                clusterCount = clusterCount + 1
                cluster = {"name":"cluster"+str(clusterCount)}

            clusterStruct = {"name":"clusters", "children":clusters}
            with open(os.path.join(output_dir, "clusters.json"), "w") as f:
                f.write(json.dumps(clusterStruct, sort_keys=True, indent=4, separators=(',', ': ')))
            #print json.dumps(clusterStruct, sort_keys=True, indent=4, separators=(',', ': '))

    except _Usage, err:
        print >>sys.stderr, sys.argv[0].split('/')[-1] + ': ' + str(err.msg)
        return 2


if __name__ == "__main__":
    sys.exit(main())