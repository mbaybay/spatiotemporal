import pandas as pd
import numpy as np
import argparse

col_names = ["geo", "month", "temp_F", "rain"]
df_index = ["month", "geo"]
start_months = ['01-2015', '03-2015', '05-2015', '08-2015', '10-2015']

def init_climate_df(filename):
    climate_df = pd.read_csv(filename, sep="\t", header=None)
    climate_df.columns = col_names
    climate_df.set_index(df_index, inplace=True)
    climate_df = climate_df.sort_values('temp_F').sort_index(level=[0,1], sort_remaining=False)
    return climate_df


def generate_travel_plan(climate_df):
    # placeholder for results
    travel_plan = pd.DataFrame()

    # iterate over months 
    for m in climate_df.index.get_level_values('month').unique():
        if m in start_months: 
            # exclude start for removing i_max
            if m != '01-2015':
                climate_df.drop(i_max, level='geo', inplace=True)
            # always update i_max if start_month
            i_max = (climate_df.loc[m]['temp_F']).argmax()
        # add to travel plan
        travel_plan = travel_plan.append(climate_df.loc[m, i_max])
    return travel_plan.reset_index()

def export_plan(climate_df, outfile):
    climate_df = climate_df.round(2)
    with open(outfile, 'w+') as f:
        f.write("(Date, Geo)\tRain(%)\tTemp(F)\n")
        for row in climate_df.as_matrix():
            line = ' '.join(map(str, row))
            f.write("%s\n" % line)
    f.close()

def main(args):
    # parse file
    df = init_climate_df(args.f)
    # generate viz of content
    plan = generate_travel_plan(df)
    # export to user-given file
    export_plan(plan, args.o)

def parse_args():
    parser = argparse.ArgumentParser(usage="Given top climate information for each of 5 regions throughout the year,\n"
                                           + "generates a travel schedule" + "Please indicate the input file (-f) and output file (-o).")
    parser.add_argument("-f", nargs='?', help='Input File path of Travel MapReduce job', required=True)
    parser.add_argument("-o", nargs='?', help='Output Directory for Travel Schedule', required=True)
    return parser.parse_args()


if __name__ == '__main__':
    # parse filename from command line
    args = parse_args()
    main(args)
