from operator import ge
import os 
import argparse
import seaborn as sns
import pandas as pd
import matplotlib.pyplot as plt
from Utilities.preprocessing import read_to_pandas
from Utilities.l1 import get_per_frame, to_trace
from Utilities.misc import isNaN

if __name__=="__main__":
    parser = argparse.ArgumentParser(description='Parse L1 data from Janus debugger data')
    parser.add_argument('--data_path', type=str, default='', help='The path to the L2 data file')
    parser.add_argument('--verbose', action='store_true', help='Print debug info')
    parser.add_argument('--save_trace', action='store_true', help='Save to Chrome trace viewer file')
    args = parser.parse_args()

    data = read_to_pandas(args.data_path)
    if args.verbose:
        print("Converted txt to pandas data frame")
    assert 'dlSchPdu' in data.keys() and 'ulSchPdu' in data.keys(), "Input file is not valid L1 data"
    # Somehow DL data is not well sorted after preprocessing
    data['dlSchPdu'] = data['dlSchPdu'].sort_values(by=['Timestamp'], ascending=True, ignore_index=True)
    if args.verbose:
        for k, v in data.items():
            print(f"\nData type: {k}")
            print(v)
    for ft in data.keys():
        data[ft]['Timestamp(ms)'] = [item/1e6 for item in data[ft]['Timestamp'].tolist()]
        data[ft]['Timestamp(s)'] = [item/1e3 for item in data[ft]['Timestamp(ms)'].tolist()]

    # Convert to relative sfn
    sfn_acc = 0
    new_sfn = [data['ulSchPdu']['sfn'][0]]
    for ii in range(1, len(data['ulSchPdu'])):
        if data['ulSchPdu']['sfn'][ii]<data['ulSchPdu']['sfn'][ii-1]:
            sfn_acc += ((data['ulSchPdu']['Timestamp'][ii]-data['ulSchPdu']['Timestamp'][ii-1]) // 1024e7 + 1) * 1024
        new_sfn.append(data['ulSchPdu']['sfn'][ii] + sfn_acc)
    data['ulSchPdu']['sfn'] = new_sfn
    sfn_acc = 0
    new_sfn = [data['dlSchPdu']['sfn'][0]]
    for ii in range(1, len(data['dlSchPdu'])):
        if data['dlSchPdu']['sfn'][ii]<data['dlSchPdu']['sfn'][ii-1]:
            sfn_acc += ((data['dlSchPdu']['Timestamp'][ii]-data['dlSchPdu']['Timestamp'][ii-1]) // 1024e7 + 1) * 1024
        new_sfn.append(data['dlSchPdu']['sfn'][ii] + sfn_acc)
    data['dlSchPdu']['sfn'] = new_sfn
    if args.verbose:
        print("Converted to relative SFN")

    # Convert DL nTbSize
    data['dlSchPdu']['nTbSize'] = [data['dlSchPdu']['nTbSize'][ii][0] for ii in range(len(data['dlSchPdu']))]
     
    # Get per frame time series
    ul_per_frame_data = get_per_frame(data['ulSchPdu'])
    dl_per_frame_data = get_per_frame(data['dlSchPdu'])
    if args.verbose:
        print(f'Generated per frame data')

    # Get time interval data
    ul_frame_interval = pd.DataFrame({'intervals': [(data['ulSchPdu']['Timestamp'][ii]-data['ulSchPdu']['Timestamp'][ii-1])/1e6 for ii in range(1, len(data['ulSchPdu']))], 
                                      'Timestamp': [data['ulSchPdu']['Timestamp'][ii]/1e9 for ii in range(1, len(data['ulSchPdu']))]})
    dl_frame_interval = pd.DataFrame({'intervals': [(data['dlSchPdu']['Timestamp'][ii]-data['dlSchPdu']['Timestamp'][ii-1])/1e6 for ii in range(1, len(data['dlSchPdu']))], 
                                      'Timestamp': [data['dlSchPdu']['Timestamp'][ii]/1e9 for ii in range(1, len(data['dlSchPdu']))]})
    # Somehow DL frames are not well sorted after this
    if args.verbose:
        print(f'Generated PDU interval data')
        print(dl_frame_interval)

    if args.save_trace:
        to_trace(data, './out/trace.json')

    # Visualize 
    plt.figure()
    sns.set_theme(style="whitegrid")
    # Bytes sent v.s. SFN
    plt.subplot(211)
    sns.lineplot(data=ul_per_frame_data, x="sfn", y="Bytes sent")
    plt.xlabel("SFN")
    plt.ylabel("Bytes sent")

    plt.subplot(212)
    sns.lineplot(data=dl_per_frame_data, x="sfn", y="Bytes sent", color='r')
    plt.xlabel("SFN")
    plt.ylabel("Bytes sent")

    plt.figure()
    sns.set_theme(style="whitegrid")
    # Frame interval v.s. time
    plt.subplot(211)
    sns.lineplot(data=ul_frame_interval, x="Timestamp", y="intervals")
    plt.xlabel("Time (s)")
    plt.ylabel("MAC PDU intervals (ms)")

    plt.subplot(212)
    sns.lineplot(data=dl_frame_interval, x="Timestamp", y="intervals", color='r')
    plt.xlabel("Time (s)")
    plt.ylabel("MAC PDU intervals (ms)")

    plt.figure()
    # Num of empty consecutive frames CDF
    plt.subplot(221)
    sns.ecdfplot(data=ul_frame_interval, x='intervals')
    plt.xlabel('MAC PDU intervals (ms)')
    plt.ylabel("")

    # TB size CDF
    plt.subplot(222)
    sns.ecdfplot(data=data['ulSchPdu'], x='nTbSize')
    plt.xlabel('TB size (byte)')
    plt.ylabel("")

    # Num of empty consecutive frames CDF
    plt.subplot(223)
    sns.ecdfplot(data=dl_frame_interval, x='intervals', color='r')
    plt.xlabel('MAC PDU intervals (ms)')
    plt.ylabel("")

    # TB size CDF
    plt.subplot(224)
    sns.ecdfplot(data=data['dlSchPdu'], x='nTbSize', color='r')
    plt.xlabel('TB size (byte)')
    plt.ylabel("")

    plt.show()
