
##### Pyspark #####


#FIlter singular column
df.filter(df.state == "OH").show(truncate=False)

# Filter multiple condition
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
