import tika
import os
tika.initVM()
from tika import parser
import operator
import tempfile
import json
import sys
import getopt




_helpMessage = '''
Usage: tsvtojson [-v] [-t tsv file] [-j json file] [-o object type] [-c column headers txt file] [-u unique field] [-e encoding file]
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
-e, --encoding
    Use the provided encodings to speed up parsing
'''


class _Usage(Exception):
    '''An error for problems with arguments on the command line.'''
    def __init__(self, msg):
        self.msg = msg
        


def clusterScores(fpRead):
    threshold = 0.01
    prior = None
    clusters = []
    clusterCount = 0
    cluster = {"name":"cluster"+str(clusterCount)}
    clusterData = []
    
    for line in fpRead:
        featureDataList = line.split(",",1) # file name,score, metadata
        if len(featureDataList) != 2:
            continue

        if prior != None:
            diff = prior-float(featureDataList[1])
        else:
            diff = -1.0
        # cleanse the \n
        featureDataList[1] = featureDataList[1].strip()
        featureData = {"name":featureDataList[0], "score":float(featureDataList[1])}

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
            #print (featureDataList[2])
            
    #add the last cluster into clusters
    cluster["children"] = clusterData
    clusters.append(cluster)
    clusterCount = clusterCount + 1
    cluster = {"name":"cluster"+str(clusterCount)}
    
    clusterStruct = {"name":"clusters", "children":clusters};
    
    fpWrite = open("clusters.json",'w+');
    fpWrite.write(json.dumps(clusterStruct, sort_keys=True, indent=4, separators=(',', ': ')));
    fpWrite.close();
        
    #print ( json.dumps(clusterStruct, sort_keys=True, indent=4, separators=(',', ': '))    )

def tikaImageSimilar():
    union_feature_names = set();
    file_parsed_data = {};
    resemblance_scores={};
    #for filename in os.listdir(pngDir):
    for filename in os.listdir("."):
        if not os.path.isfile(filename) or not ".jpg" in filename:
            continue
        # first compute the union of all features
        parsedData = parser.from_file(filename)
        file_parsed_data[filename] = parsedData["metadata"]
        union_feature_names = union_feature_names | set(parsedData["metadata"].keys())
    total_num_features = len(union_feature_names)
    # now compute the specific resemblance and containment scores
    for filename in file_parsed_data.keys():
        overlap = {}
        overlap = set(file_parsed_data[filename].keys()) & set(union_feature_names) 
        resemblance_scores[filename] = float(len(overlap))/total_num_features

    sorted_resemblance_scores = sorted(resemblance_scores.items(), key=operator.itemgetter(1), reverse=True)
    #print "Resemblance:\n"

    test = tempfile.TemporaryFile(mode='w+t');
    startWrite = False;    
    for tuple in sorted_resemblance_scores:
        if startWrite == False:
            test.writelines( tuple[0]+","+str(tuple[1]) );
            startWrite = True;
        else:
            test.writelines( "\n" + tuple[0]+","+str(tuple[1]) );
            
    test.seek(0);
    clusterScores(test);
    test.close();






def main(argv=None):
        
    """
    #opts, args = getopt.getopt(argv[1:],'hvt:j:c:o:u:e:',['help', 'verbose', 'tsv=','json=','cols=','object=', 'unique=', 'encoding='])
    try:
        opts, args = getopt.getopt(argv[1:],'hd',['help', 'dir'])
    except getopt.error, msg:
        raise _Usage(_helpMessage)    
     
    if len(opts) == 0:
        raise _Usage(_helpMessage)
    
    for option, value in opts:
        if option in ('-h', '--help'):
            raise _Usage(_helpMessage)
        elif option in ('-d', '--dir'):
            pngFileDir = value
    """
       
       
    tikaImageSimilar();
       

    


if __name__ == "__main__":
    sys.exit(main())
    
