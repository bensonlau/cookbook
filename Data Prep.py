from sklearn import datasets
#https://scikit-learn.org/0.15/modules/classes.html#module-sklearn.datasets


##### Pyspark ######aßßssssssssss

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


# Adding & Removing
df.drop() #column name

#Renaming
df = df.withColumnRenamed("existing","new")

#Casting
df.withColumn("age",df.age.cast('integer'))

#Merging

df = df.join(spark_table, df.store_Id == spark_table.store_Id2,"left")
df = df.join(spark_table,["store_id"])