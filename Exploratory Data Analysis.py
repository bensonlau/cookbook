##### Pyspark #####

#summary statistics of columns present in the data frame (df)
#add .display() in databricks for friendlier visualization
df.describe() 
df.summary() #functions like describe() but provides values at 25th, 50th, 75th percentiles

print('Data overview')
df.printSchema()

print('Columns overview')
pd.DataFrame(df.dtypes, columns = ['Column Name','Data type'])

#Filter singular column
df.filter(df.state == "OH").show(truncate=False)

# Filter multiple conditi don
df.filter( (df.state  == "OH") & (df.gender  == "M") ) \
    .show(truncate=False) 

# Counting distinct values from a column
df.select('column name').distinct().collect()

##### Pandas #####
import pandas as pd
# Check data type of all columns
df.dtypes

# Counting the length of column
df['[column_name]']str.len()

#Counting the number of rows & columns of a dataframe
#...number of rows
rows = len(df.axes[0])
 
#...number of columns
cols = len(df.axes[1])
print("Number of Rows: ", rows)
print("Number of Columns: ", cols)
        
# - Below are some transformations you can perform on Pandas DataFrame. Note that statistical functions calculate at each column by default. you don’t have to explicitly specify on what column you wanted to apply the statistical functions. Even count() function returns count of each column (by ignoring null/None values).

df.count() # – Returns the count of each column (the count includes only non-null values).
df.corr() # – Returns the correlation between columns in a data frame.
df.head(n) # – Returns first n rows from the top.
df.max() # – Returns the maximum of each column.
df.mean() # – Returns the mean of each column.
df.median() # – Returns the median of each column.
df.min() # – Returns the minimum value in each column.
df.std() # – Returns the standard deviation of each column
df.tail(n) # – Returns last n rows.

df["column_name"].mean()
df["column_name"].median()
df["column_name"].describe()

#Grouping

# counting unique values
n = len(pd.unique(df['height']))
print("No.of.unique values :", n)

#Filtering
rslt_df = dataframe[dataframe['column_name'] > 70])

#Pivoting
pivoted_df = pd.pivot_table(df,
  values='DIGITAL_UNITS',
  index=["ZIP_CODE"],
  columns=['FOP_GLOBAL'],aggfunc='sum').reset_index()