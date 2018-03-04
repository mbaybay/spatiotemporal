import argparse
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import os
import seaborn as sns


def generate_hist(df, outpath):
    fig, ax = plt.subplots()
    fig.set_size_inches(8.5, 6)

    hist = sns.barplot(x="Month", y="Precipitation(kg/m2)", data=df)
    hist.set_xticklabels(hist.get_xticklabels(), rotation=30)
    hist.set_title("Bay Area's Average Precipitation in 2015")
    for p in hist.patches:
        hist.annotate(np.round(p.get_height(), decimals=2),
                      (p.get_x() + p.get_width() / 2., p.get_height()),
                      ha='center', va='center')

    fig.savefig(outpath + os.sep + "bayarea_hist.png")


def get_data(filename):
    try:
        os.path.isfile(filename)
    except IOError:
        print("File Not Found: ", filename)

    data = pd.read_csv(filename, sep="\t", header=None)
    data.columns = ["Month", "Precipitation(kg/m2)"]
    data.sort_values(by="Month", inplace=True)
    print(data)
    return data


def main(args):
    # parse file
    data_df = get_data(args.f)
    # generate viz of content
    generate_hist(data_df, args.o)


def parse_args():
    parser = argparse.ArgumentParser(usage="Generates a histogram of precipitation in the Bay Area. \n"
                                           + "Please indicate the input file (-f).")
    parser.add_argument("-f", nargs='?', help='Input File path of BayArea MapReduce job', required=True)
    parser.add_argument("-o", nargs='?', help='Output Directory for plot', required=True)
    return parser.parse_args()


if __name__ == '__main__':
    # parse filename from command line
    args = parse_args()
    main(args)