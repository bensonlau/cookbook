##### Pyspark #####

#summary statistics of columns present in the data frame
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

##### Pandas #####
# Printing the percentage of missing values per column
def percent_missing(pandasDf):
    '''
    Prints the percentage of missing values for each column in a dataframe
    '''
    # Summing the number of missing values per column and then dividing by the total rows
    sumMissing = dataframe.isnull().values.sum(axis=0)
    pctMissing = sumMissing / dataframe.shape[0]
    
    if sumMissing.sum() == 0:
        print('No missing values')
    else:
        # Looping through and printing out each columns missing value percentage
        print('Percent Missing Values:', '\n')
        for idx, col in enumerate(dataframe.columns):
            if sumMissing[idx] > 0:
                print('{0}: {1:.2f}%'.format(col, pctMissing[idx] * 100))
        
df.head()
df["column_name"].mean()
df["column_name"].median()
df["column_name"].describe()