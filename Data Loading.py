from sklearn import datasets
#https://scikit-learn.org/0.15/modules/classes.html#module-sklearn.datasets


##### Pyspark ######

#Loading Data
query = """[sql query]]"""
df = spark.sql(query)

df = spark.read.format('csv').option('header','true').load('[s3://desired_path]')

##Joining Data
df3 = df1.join(df2, df1.colName ==  df2.colName,"inner")

#Saving Data
df3.write.mode("overwrite").option("path", "s3://desired_path]").saveAsTable("schema.tableName")

# Adding & Removing
df.drop() #column name

#Renaming
df = df.withColumnRenamed("existing","new")

#Merging

df = df.join(spark_table, df.store_Id == spark_table.store_Id2,"left")
df = df.join(spark_table,["store_id"])