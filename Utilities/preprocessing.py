import json, ast
import os
import pandas as pd
from operator import itemgetter


'''
    Janus data info: the key is the frame type name;
    The first tuple is the field names for frame and slot,
    The second is for frame data
'''
frame_types = {'boReport': (('sfn', 'slot'), ("rnti", "lcId", "queueLoad")), 
               'bsrReport': (('sfn', 'slot'), ("rnti", "lcgBitmask", "bsrNet")), 
               'csiReport': (('sfn', 'slot'), ("rnti", "pmiI11", "pmiI12", "pmiI13", "pmiI2", "pmiIdx", "ri", "cqi")),
               'ulSchPdu': (('frame', 'slot'), ("rnti", "mcs", "nLayers", "nAntennaP", "nPmi", "raType", "rbStart", "rbSize", "nTbSize", "rv", "ndi", "nRetx", "nHarqid")),
               'dlSchPdu': (('frame', 'slot'), ("rnti", "raType", "rbStart", "rbSize", "nRbg", "nRbgSize", "mcs", "rv", "ndi", "nLayers", "nAntennaP", "nTbSize", "nRetx", "nHarqid"))}

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
    with open(f'tmp_json', 'w') as f:
        for item in data:
            payload_str = str(item[2])
            payload_str = payload_str.replace("\"", "\\\"")
            f.write(''.join(["[{\"stream_id\": \"", str(item[0]), "\", ", 
                              "\"stream_type\": \"janus\", ", 
                              "\"stream_sn\": ", str(item[1]), ", "
                              "\"stream_payload\": \"", payload_str, "\"}]\n"]))

def parse_line(line):
    '''
    Read one line of JSON file

    '''
    json_file = json.loads(line.strip())
    data={}
    for temp in json_file:
        if 'stream_payload' in temp:
            stream_sn = int(temp["stream_sn"])
            temp = ast.literal_eval(temp['stream_payload'])
            for ft in frame_types.keys():
                if ft in temp:
                    frame_type = ft
            if 'frame_type' not in locals():
                raise KeyError('Message type not recognized')
            data = temp[frame_type][0]
            timestamp = temp['timestamp']
            sfn = temp[frame_types[frame_type][0][0]]
            slot = temp[frame_types[frame_type][0][1]]
    return frame_type, data, timestamp, sfn, slot

def read_to_pandas(data_fn):
    '''
    Read Janus debugger data from a file and return a list of pandas dataframes
    
    '''
    convert(data_fn)
    with open(f'tmp_json', 'r') as f:
        lines = f.readlines()
        data_p = {}
        for line in lines:
            frame_type, data, t, sfn, slot = parse_line(line)
            if frame_type not in data_p.keys():
                data_p[frame_type]={field:[] for field in frame_types[frame_type][1]}
                data_p[frame_type]['Timestamp']=[]
                data_p[frame_type]['sfn']=[]
                data_p[frame_type]['slot']=[]
            data_p[frame_type]['Timestamp'].append(int(t))
            data_p[frame_type]['sfn'].append(int(sfn))
            data_p[frame_type]['slot'].append(int(slot))
            for field in frame_types[frame_type][1]:
                if field in data.keys():
                    data_p[frame_type][field].append(data[field])
                else:
                    data_p[frame_type][field].append(float('nan'))
        # os.remove('tmp_json')

    data_p = {ft:pd.DataFrame(data_p[ft]) for ft in data_p.keys()}
    # Somehow DL data is not well sorted after preprocessing
    if 'dlSchPdu' in data_p.keys():
        data_p['dlSchPdu'] = data_p['dlSchPdu'].sort_values(by=['Timestamp'], ascending=True, ignore_index=True)
    return data_p