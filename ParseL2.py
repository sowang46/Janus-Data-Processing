import os 
import argparse
import seaborn as sns
import matplotlib.pyplot as plt
from Utilities.preprocessing import read_to_pandas

def isNaN(item):
    return item!=item

if __name__=="__main__":
    parser = argparse.ArgumentParser(description='Parse L2 data from Janus debugger data')
    parser.add_argument('--data_path', type=str, default='', help='The path to the L2 data file')
    parser.add_argument('--verbose', type=bool, default=False, help='Print debug info')
    parser.add_argument('--relative_time', action='store_true', help='Show relative timestamp starting from 0')
    args = parser.parse_args()

    data = read_to_pandas(args.data_path)
    assert 'boReport' in data.keys(), "Input file is not L2 data"
    if args.verbose:
        for k, v in data.items():
            print(f"\nData type: {k}")
            print(v)
    if args.relative_time:
        t_min = min([min(data[ft]['Timestamp']) for ft in data.keys()])
        for ft in data.keys():
            data[ft]['Timestamp'] = [item-t_min for item in data[ft]['Timestamp'].tolist()]
    for ft in data.keys():
        data[ft]['Timestamp(ms)'] = [item/1e6 for item in data[ft]['Timestamp'].tolist()]
        data[ft]['Timestamp(s)'] = [item/1e3 for item in data[ft]['Timestamp(ms)'].tolist()]

    # Visualize
    # Convert all list in 'bsrNet' coloum to int 
    data['bsrReport']['bsrNet'] = [item[0] if not isNaN(item) else item
                                    for item in data['bsrReport']['bsrNet'].tolist()]
    plt.figure()
    sns.set_theme(style="whitegrid")
    sns.lineplot(data=data['bsrReport'], x='Timestamp(s)', y='bsrNet')

    data['boReport']['queueLoad'] = [item[0] if not isNaN(item) else item
                                    for item in data['boReport']['queueLoad'].tolist()]
    plt.figure()
    sns.lineplot(data=data['boReport'], x='Timestamp(s)', y='queueLoad')

    plt.figure()
    sns.lineplot(data=data['csiReport'], x='Timestamp(s)', y='cqi')
    plt.show()