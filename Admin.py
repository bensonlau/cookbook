import sys, os, time, numpy as np, pandas as pd

print(time.strftime('%Y/%m/%d %H:%M'))
print('OS:', sys.platform)
print('CPU Cores:', os.cpu_count())
print('Python:', sys.version)
print('NumPy:', np.__version__)
print('Pandas:', pd.__version__)

[object].lower()
[object].upper()

#Local variables used for saving
project_name = "project_name"
folder_location = 's3://[folder]/[project_name]/'
file_path = folder_location + project_name +"_"+ curr_date
schema_name="blau8"
schema_table = schema_name +"."+ project_name+"_"+curr_date

######Pyspark
#check type of object
type(df)

#check data types of data frame
df.dtypes

print('Data overview')
df.printSchema()

######Pandas
print('Columns overview')
pd.DataFrame(df.dtypes, columns = ['Column Name','Data type'])

pd.set_option('display.max_columns', None)
pd.options.display.max_rows=1000