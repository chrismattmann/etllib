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
from etl.etllib import compareKeySimilarity, compareValueSimilarity, convertKeyUnicode, generateCirclePacking, generateCluster, generateLevelCluster

_verbose = False
_helpMessage = '''

Usage: similarity [-v --verbose] [-h --help] <operation> [--threshold <threshold>] [--maxnode <maxNumberOfNode>] [-f --directory <directory of files>] [-c --file <file1 file2>] 

Operation:
DEFAULT IS --key
--key
    compare similarity by using file metadata key 

--value
    compare similarity by using file metadata value

Options:  
--threshold [value of threshold] <default threshold = 0.01>
    set threshold for cluster similarity   
--maxnode [value of number of nodes] <default num = 10>
    set max num of node for each cluster node
-o, --output
    the directory of output files
-f, --directory [path to directory]
    read similarity-scores.txt file from this directory
-c, --file [file1 file2]
    compare given files
-v, --verbose
    Work verbosely rather than silently.
-h --help
    show help on the screen

Example:
similarity.py -f [directory of files]
similarity.py -o [directory of output files] -f [directory of files]
similarity.py --key  --threshold [threshold] -f [directory of files]
similarity.py --threshold [threshold] --maxnode [maxNumberOfNode] -f [directory of files]
similarity.py -c [file1 file2]
similarity.py -o [directory of output files] -c [file1 file2]
similarity.py --key  --threshold [threshold] -c [file1 file2]
similarity.py --threshold [threshold] --maxnode [maxNumberOfNode] -c [file1 file2]

'''

def verboseLog(message):
    if _verbose:
        print(message)

class _Usage(Exception):
    ''' an error for arguments '''

    def __init__(self, msg):
        self.msg = msg

def main(argv = None):
    if argv is None:
        argv = sys.argv

    try:
        try:
            opts, args = getopt.getopt(argv[1:], 'f:c:t:o:vhkrm:', ['directory=', 'file=', 'threshold=', 'output=', 'verbose', 'help', 'key', 'value', 'maxnode='])
        except getopt.error as msg:
            raise _Usage(msg)

        if len(opts) ==0:
            raise _Usage(_helpMessage)

        threshold = 0.01
        output_dir = ""
        filenames = []
        filename_list = []
        dirFile = ""
        flag = 1
        maxNumberOfNode = 10

        for option, value in opts:

            if option in ('--key'):
                flag = 1
            elif option in ('--value'):
                flag = 2

            elif option in ('-v', '--verbose'):
                global _verbose
                _verbose = True
            elif option in ('-h', '--help'):
                raise _Usage(_helpMessage)
            elif option in ('--threshold'):
                threshold = float(value)
            elif option in ('--maxnode'):
                maxNumberOfNode = value
            elif option in ('-o', '--output'):
                output_dir = value
            elif option in ('-c', '--file'):
                index = argv.index('--file')
                filenames = argv[1+index : ]
            elif option in ('-f', '--directory'):
                dirFile = value
                filenames=[filename for filename in os.listdir(dirFile) if not filename.startswith('.')]                


        #format the filenames
        filenames = [x.strip() for x in filenames]
        filenames = [filenames[k].strip('\'\n') for k in range(len(filenames))]

        for filename in filenames :
            if not os.path.isfile(os.path.join(dirFile, filename)):
                continue
            filename = os.path.join(dirFile, filename) if dirFile else filename
            filename_list.append(filename)

        if len(filename_list) <2 :
            raise _Usage("you need to type in at least two valid files")

        similarity_score = []
        if flag == 1:
            print("KEY BASED SIMILARITY")
            sorted_resemblance_scores, file_parsed_data = compareKeySimilarity(filename_list)
        elif flag ==2:
            print("VALUE BASED SIMILARITY")
            sorted_resemblance_scores, file_parsed_data = compareValueSimilarity(filename_list)

        for tuple in sorted_resemblance_scores:
            similarity_score.append(os.path.basename(tuple[0].rstrip(os.sep))+","+str(tuple[1]) + "," + tuple[0] + "," + convertKeyUnicode(file_parsed_data[tuple[0]])+'\n')

        with open(os.path.join(output_dir, "similarity-scores.txt"), "w") as f:
	    f.write("Resemblance : \n")
	    for tuple in sorted_resemblance_scores:
                f.write(os.path.basename(tuple[0].rstrip(os.sep)) + ","+str(tuple[1]) + "," + tuple[0] + "," + convertKeyUnicode(file_parsed_data[tuple[0]]) + '\n')
            
        #generate cluster json
        clusterStruct = generateCluster(similarity_score, threshold)
        clusterStruct = json.dumps(clusterStruct, sort_keys=True, indent=4, separators=(',', ': '))
        circlePackingStruct = generateCirclePacking(similarity_score, threshold)
        levelClusterStruct = generateLevelCluster(clusterStruct)

        with open(os.path.join(output_dir, "clusters.json"), "w") as f:
            f.write(clusterStruct)
        with open(os.path.join(output_dir, "levelCluster.json"), "w") as f:
            f.write(json.dumps(levelClusterStruct, sort_keys=True, indent=4, separators=(',', ': ')))
        with open(os.path.join(output_dir, "circle.json"), "w") as f:
            f.write(json.dumps(circlePackingStruct, sort_keys=True, indent=4, separators=(',', ': ')))

    except _Usage as err:
        print(sys.argv[0].split('/')[-1] + ': ' + str(err.msg))
        return 2


if __name__ == "__main__":
    sys.exit(main())
