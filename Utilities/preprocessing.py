import json
from operator import itemgetter


def convert(data_fn):
    '''
    Convert the janus debugger to the format of MinerVa app output
    The output is stored in a file called tmp_json

    :data_fn: data file name
    '''
    data = []
    with open(data_fn) as f:
        for ii in range(10):
            next(f)
        lines = f.readlines()
        for line in lines:
            temp = line.split()
            stream_id = temp[0]
            stream_sn = int(temp[1])
            payload = ''.join(temp[2:len(temp)])
            data.append((stream_id, stream_sn, payload))
        data = sorted(data, key=itemgetter(1))
    # Write to JSON
    with open(f'tmp_json', 'a') as f:
        for item in data:
            payload_str = str(item[2])
            payload_str = payload_str.replace("\"", "\\\"")
            f.write(''.join(["[{\"stream_id\": \"", str(item[0]), "\", ", 
                              "\"stream_type\": \"janus\", ", 
                              "\"stream_sn\": ", str(item[1]), ", "
                              "\"stream_payload\": \"", payload_str, "\"}]\n"]))

if __name__=="__main__":
    convert('./../data/0809/0809_l2_codelets_test_1.txt')