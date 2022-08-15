import pandas as pd

def get_per_frame(data):    
    '''
    Convert data stream to per frame series

    '''
    per_frame_data = {'sfn': [], '# RBs': [], 'Bytes sent': [], 'Max MCS': [], 'Max RV': []}
    curr_sfn = data['sfn'][0]
    rb_counter = data['rbSize'][0]
    byte_counter = data['nTbSize'][0]
    curr_max_mcs = data['mcs'][0]
    curr_max_rv = data['rv'][0]
    for ii in range(1, len(data)):
        if data['sfn'][ii]>curr_sfn or ii==len(data)-1:
            per_frame_data['sfn'].append(curr_sfn)
            per_frame_data['# RBs'].append(rb_counter)
            per_frame_data['Bytes sent'].append(byte_counter)
            per_frame_data['Max MCS'].append(curr_max_mcs)
            per_frame_data['Max RV'].append(curr_max_rv)
            if data['sfn'][ii]-curr_sfn>1:
                while curr_sfn<data['sfn'][ii]-1:
                    curr_sfn += 1
                    per_frame_data['sfn'].append(curr_sfn)
                    per_frame_data['# RBs'].append(0)
                    per_frame_data['Bytes sent'].append(0)
                    per_frame_data['Max MCS'].append(float("nan"))
                    per_frame_data['Max RV'].append(float("nan"))
            curr_sfn += 1
            rb_counter = data['rbSize'][ii]
            byte_counter = data['nTbSize'][ii]
            curr_max_mcs = data['mcs'][ii]
            curr_max_rv = data['rv'][ii]
        else:
            rb_counter += data['rbSize'][ii]
            byte_counter += data['nTbSize'][ii]
            if data['mcs'][ii] > curr_max_mcs:
                curr_max_mcs = data['mcs'][ii]
            if data['rv'][ii] > curr_max_rv:
                curr_max_rv = data['rv'][ii]
    per_frame_data = pd.DataFrame(per_frame_data)

    return per_frame_data