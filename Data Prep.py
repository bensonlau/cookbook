from sklearn import datasets
#https://scikit-learn.org/0.15/modules/classes.html#module-sklearn.datasets

####################
##### Pyspark ######
####################

#Viewing in databricks
display(df)

#Converting from Pandas
# SparkSession
from pyspark.sql import SparkSession
 
# Building the SparkSession and name
# it :'pandas to spark'
spark = SparkSession.builder.appName(
  "pandas to spark").getOrCreate()

sparkDF=spark.createDataFrame(pandasDF) 

#Loading Data
#...from SQL databases
query = """[sql query]]"""
df = spark.sql(query)

#...from csv
df = spark.read.format('csv').option('header','true').load('[s3://desired_path]')

#Saving Data
#....to s3 location
df.write.mode("overwrite").option("path", "s3://desired_path").saveAsTable("schema.tableName")

#...to csv
df.write.mode('overwrite').csv(file_path, header = 'true')

##Joining Data
df3 = df1.join(df2, df1.colName ==  df2.colName,"inner")

# Adding & Removing columns
df.select("column_name")
df.drop("column_name") 
df.assign() #returns a new object with all original columns in addition to new observations
  #examples:
  result = df.assign(total = df.groupby("gender")["score_points"].transform("cumsum"))


#Renaming
df = df.withColumnRenamed("existing","new")

#Casting
df.withColumn("age",df.age.cast('integer'))

#Merging
df3 = df1.join(df2, df1.store_Id == df2.store_Id2,"left")
df3 = df1.join(df2,["store_id"])


####################
##### Pandas ######
####################
import pandas as pd

#Converting from Pyspark to Pandas
pandasDf = sparkDf.toPandas()

#Copying dataset
newDf = pandasDf.copy()

#Loading data
#...from csv
df = pd.read_csv('[file_name.csv]')
#...from excel
df = pd.read_excel('[file_name.xlsx]',sheet_name = '[sheet_name]]'

#Selecting columns
df['column_name'] #returns a pandas series
df[['column_name']] #returns a pandas dataframe

#Renaming columns
df.rename(columns={'old_column_name': 'new_column_name'})

##...and rows
df[0:4] #select the first 4 observations

#Merging
df1.merge(df2, how='inner', on='shared_column_name')

#Sorting
df.sort_values(by=['col1','col2'], ascending=False)

#Grouping
df.groupby(['column_name']).reset_index(drop=True)

#...Counting distinct
df.groupby(['column_name']).agg('column_name:pd.Series.nunique).reset_index()

#Average of columns
df.groupby('column_name').agg(name_of_column = ('column','mean')).reset_index()
