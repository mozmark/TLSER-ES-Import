import json
import sys
from settings import config
from elasticsearch import Elasticsearch

if __name__ == '__main__':
    # get some settings
    index = config['index']
    doc_type = config['doc_type']
    esconfig = config['esconfig']
    pause_point = config['pause_point']
    pause_time = config['pause_time']

    # open our file
    filename = sys.argv[1]
    print 'loading data from',filename
    f = open(filename,'r')
    es = Elasticsearch(esconfig)
    es.indices.create(index=index, ignore=400)

    # import the json data into the ES instance
    count = 0
    for line in f:
        try:
            doc = json.loads(line)
            es.index(index=index, doc_type=doc_type, id=doc['doc_id'], body=doc)
            count = count + 1
            if 0 != pause_point:
                if count % 1000 == 0:
                    print count
                    if pause_time > 0:
                        time.sleep(pause_time)
        except KeyboardInterrupt:
            raise
        except:
            print 'ooops', sys.exc_info[0]
