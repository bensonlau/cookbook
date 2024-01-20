##### Pyspark #####

#summary statistics of columns present in the data frame (df)
df.describe()

print('Data overview')
df.printSchema()

print('Columns overview')
pd.DataFrame(df.dtypes, columns = ['Column Name','Data type'])

#FIlter singular column
df.filter(df.state == "OH").show(truncate=False)

# Filter multiple conditi don
df.filter( (df.state  == "OH") & (df.gender  == "M") ) \
    .show(truncate=False) 


'''
Using count() action to get the number of rows on DataFrame and len(df.columns()) to get the number of columns.
'''
def spark_shape(sparkDf):
  print(sparkDf.count(), len(sparkDf.columns))

print("spark_shape(sparkDf) loaded")
print("Using count()  to get the number of rows on DataFrame and len(df.columns()) to get the number of columns.")

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
        
df.head()
df["column_name"].mean()
df["column_name"].median()
df["column_name"].describe()

#Grouping

# counting unique values
n = len(pd.unique(df['height']))
print("No.of.unique values :", n)

#Filtering
rslt_df = dataframe[dataframe['column_name'] > 70])