import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.ticker as ticker

import pandas as pd

def create_df(file_path):
    df = pd.read_csv(file_path, sep=" ")
    df.index = df.pop('detector')
    df.sort_values(by='filesize', ascending=False, inplace=True)
    return df

def plot(df):
    plt.ion()
    col_name = df.columns.values[0]

    fig, ax = plt.subplots()
    plt.bar(df.index, df[col_name] *1e-9)
    ax.set_title('CHX Detectors')
    ax.set_xlabel('Detectors')
    ax.set_ylabel('File usgae (GB)')
    fig.autofmt_xdate(bottom=0.5, rotation=57, ha='right')
    plt.show()


file_path = '/home/jdiaz/projects/data-monitoring/exercises/chx_v2_dets_filesize.dat'
df = create_df(file_path)

# top 3 detectors
df = df.iloc[:3]
plot(df)