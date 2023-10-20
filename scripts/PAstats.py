import pandas as pd
import numpy as np
from pa_algorithm import PAbouts
import datetime
from globals import *

def get_participant_baseline_stats(subject_id):
    # get the participant start date
    start_date = start_dates_dict[subject_id]
    # get the end date of phase 1
    baseline_date = baseline_dict[subject_id]
    # if baseline date is "N/A", set it to today
    if baseline_date == "N/A":
        baseline_date = datetime.datetime.today().strftime('%Y-%m-%d')
    # turn the start date into a datetime object
    start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d')
    #turn the end date into a datetime object
    baseline_date = datetime.datetime.strptime(baseline_date, '%Y-%m-%d')
    
    # get the participant baseline AUC
    path_to_proximal_table = "/home/hle5/sciJitaiScript/reports/proximal/" + subject_id + ".csv"
    
    # read the proximal table
    proximal_table = pd.read_csv(path_to_proximal_table)
    
    # convert the "date" column to datetime object
    proximal_table['date'] = pd.to_datetime(proximal_table['date'])
    # only keep the rows that are within the baseline period
    proximal_table = proximal_table.loc[(proximal_table['date'] >= start_date) & (proximal_table['date'] <= baseline_date)]
    # get the total_pa column as a list
    total_pa_list = proximal_table['total_pa'].tolist()
    return total_pa_list

def get_baseline_stats_for_all():
    # create an empty dataframe
    baseline_df = pd.DataFrame()
    
    # get the list of participant id
    for pid in subjects[::-1]:
        # get the baseline stats for each participant
        baseline_stats = get_participant_baseline_stats(pid)
        # replace nan with "NO_DATA"
        baseline_stats = [str(x) for x in baseline_stats]
        baseline_stats = ['NO_DATA' if x == 'nan' else x for x in baseline_stats]
        # add the baseline stats to the dataframe as a new column, if there is a mismatch in length, pad the shorter list with "N/A"
        if len(baseline_stats) < len(baseline_df):
            baseline_stats = baseline_stats + ['N/A'] * (len(baseline_df) - len(baseline_stats))
        elif len(baseline_stats) >= len(baseline_df):
            baseline_df = baseline_df.reindex(range(len(baseline_stats)))
            baseline_df = baseline_df.fillna('N/A')
        baseline_df[pid] = baseline_stats
    
    # remove the rows at the tail of the table where all the values are NO_DATA
    baseline_df = baseline_df.loc[~(baseline_df == 'N/A').all(axis=1)]
    
    # create a date column at the front, and fill it from 1 to the length of the dataframe
    baseline_df.insert(0, 'date', range(1, len(baseline_df) + 1))
    # set date as index
    baseline_df.set_index('date', inplace=True)

    # create a row, which is the sum of all the numerical values in each column, ignore the "N/A" values
    baseline_df.loc['sum']=baseline_df.apply(lambda x: pd.to_numeric(x,errors='coerce')).sum(axis=0).round(3)
    #create a row, which is the number of numerical values in each column, ignore the "N/A" values, ignore the last row
    baseline_df.loc['count']=baseline_df.apply(lambda x: pd.to_numeric(x,errors='coerce')).count(axis=0).round(3)
    # minus one from the count row, take min value of 0
    baseline_df.loc['count'] = baseline_df.loc['count'] - 1
    baseline_df.loc['count'] = baseline_df.loc['count'].clip(lower=0)
    
    # create a row and take the sum/count, name it "average", if count is 0, set it to "N/A"
    baseline_df.loc['average'] = baseline_df.apply(lambda x: x['sum']/x['count'] if x['count'] != 0 else 'N/A', axis=0)
    #create a temp dataframe with the last 3 rows removed
    baseline_df_temp = baseline_df.iloc[:-3]
    # create a row called std, whihc is the standard deviation of each column in the temp df, ignore the "N/A" values
    baseline_df.loc['std'] = baseline_df_temp.apply(lambda x: pd.to_numeric(x,errors='coerce')).std(axis=0).round(3)
    # round any numerical value to 3 decimal places
    baseline_df = baseline_df.applymap(lambda x: round(x, 3) if isinstance(x, (int, float)) else x)
    # replace NaN string with "N/A"
    baseline_df = baseline_df.replace(np.nan, 'N/A', regex=True)
    # save the report as baseline_stats.csv in the reports folder
    baseline_df.to_csv("/home/hle5/sciJitaiScript/reports/baseline_stats.csv")    
    return baseline_df

# print(get_baseline_stats_for_all())