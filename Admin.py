import sys, os, time, numpy as np, pandas as pd

print(time.strftime('%Y/%m/%d %H:%M'))
print('OS:', sys.platform)
print('CPU Cores:', os.cpu_count())
print('Python:', sys.version)
print('NumPy:', np.__version__)
print('Pandas:', pd.__version__)


[object].lower()
[object].upper()

#Pyspark
#check data types
df.dtypes