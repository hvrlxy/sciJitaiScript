import os
import pandas as pd
from pandas import ExcelWriter
from pandas import ExcelFile

PATH_TO_CSV_FOLDER='/home/hle5/sciJitaiScript/reports/proximal/'

# list all the csv files in the folder
csv_files = [f for f in os.listdir(PATH_TO_CSV_FOLDER) if os.path.isfile(os.path.join(PATH_TO_CSV_FOLDER, f)) and f.endswith('.csv')]
# sort the csv files
csv_files.sort()

# initialize the excel writer
writer = ExcelWriter('proximal.xlsx')

# loop through the csv files and read them into a dataframe
for csv_file in csv_files:
    df = pd.read_csv(PATH_TO_CSV_FOLDER + csv_file)
    # write the dataframe to a sheet in the excel file
    df.to_excel(writer, csv_file[:-4], index=False)
# save the excel file
writer.save()