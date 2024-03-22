import gzip
import json

def get_extra_data_file(jsonfilename):
    with gzip.open(jsonfilename, 'r') as fin:
        line_data = fin.readlines()

    data = []
    for i in line_data:
        data.append(json.loads(i))

    return data