from sklearn import datasets
#https://scikit-learn.org/0.15/modules/classes.html#module-sklearn.datasets

####################
##### Pyspark ######
####################

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

#Sorting
df.sort_values(by=['col1','col2'], ascending=False)

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

#Merging
df3 = df1.merge(df2, how='inner', on='a')

def confirmation_rate(signups: pd.DataFrame, confirmations: pd.DataFrame) -> pd.DataFrame:
    return (
        pd.merge(signups[["user_id"]], confirmations[["action", "user_id"]], on="user_id", how="left")
        .assign(confirmation_rate=lambda frame: frame.action.apply(lambda action: action == "confirmed"))
        .groupby("user_id", as_index=False)
        .confirmation_rate
        .mean()
        .round(2)
    )