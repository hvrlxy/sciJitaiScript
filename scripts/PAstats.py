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
    for pid in subjects:
        # get the baseline stats for each participant
        baseline_stats = get_participant_baseline_stats(pid)
        # create a dataframe for the baseline stats
        new_pid = pd.DataFrame(baseline_stats, columns=[pid])
        #concatenate the new dataframe to the baseline dataframe
        baseline_df = pd.concat([baseline_df, new_pid], axis=1)
    
    # remove the rows at the tail of the table where all the values are NO_DATA
    baseline_df = baseline_df.loc[~(baseline_df == 'NO_DATA').all(axis=1)]
    
    # create a date column at the front, and fill it from 1 to the length of the dataframe
    baseline_df.insert(0, 'date', range(1, len(baseline_df) + 1))
    # set date as index
    baseline_df.set_index('date', inplace=True)

    # turn all NO_DATA to NaN
    baseline_df = baseline_df.replace('NO_DATA', np.nan)
    # turn all values into int  
    baseline_df = baseline_df.astype(float)
    
    # add a another row, with "mean" as the index, and the value is the mean of each column, ignoring the NaN
    baseline_df.loc['mean'] = baseline_df.mean(axis=0, skipna=True)
    # add another row, with "std" as the index, and the value is the std of each column, ignoring the NaN
    baseline_df.loc['std'] = baseline_df.std(axis=0, skipna=True)
    # add a row with index "num_valid", and the values is the number of days with no NaN values
    baseline_df.loc['num_days_valid'] = baseline_df.count(axis=0)
    # minus 2 from the num_days_valid row, because the last 2 rows are mean and std
    baseline_df.loc['num_days_valid'] = baseline_df.loc['num_days_valid'] - 2
    # reformat the float values to 2 decimal places
    baseline_df = baseline_df.round(2)
    
    #replace NaN values with NO_DATA
    baseline_df = baseline_df.replace(np.nan, 'NO_DATA')
    
    # save the report as baseline_stats.csv in the reports folder
    baseline_df.to_csv("/home/hle5/sciJitaiScript/reports/baseline_stats.csv")    
    return baseline_df