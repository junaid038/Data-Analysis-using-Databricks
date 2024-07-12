# Databricks notebook source
#df=spark.read.csv("/FileStore/tables/ADSI_Table_1A_12.csv")
#df.show()
#display(df)

from pyspark.sql.types import StructType,StringType,IntegerType,StructField

accident_schema=StructType([StructField('Sl.no.',IntegerType(),True),StructField('State',StringType(),True),StructField('Collisions - Cases', IntegerType(), True), StructField('Collisions - Injured', IntegerType(), True), StructField('Collision - Died', IntegerType(), True), StructField('Derailments -Cases', IntegerType(), True), StructField('Derailment - Injured', IntegerType(), True), StructField('Derailment - Died', IntegerType(), True), StructField('Fall from Train/Collision with People at Tracks - Cases', IntegerType(), True), StructField('Fall from Train/Collision with People at Tracks - Injured', IntegerType(), True), StructField('Fall from Train/Collision with People at Tracks - Died', IntegerType(), True), StructField('Explosion/Fire - Cases', IntegerType(), True), StructField('Explosion/Fire - Injured', IntegerType(), True), StructField('Explosion/Fire - Died', IntegerType(), True), StructField('Others - Cases', IntegerType(), True), StructField('Others - Injured', IntegerType(), True), StructField('Others - Died', IntegerType(), True), StructField('Total - Cases', IntegerType(), True), StructField('Total - Injured', IntegerType(), True), StructField('Total - Died', IntegerType(), True)])

df=spark.read.csv("/FileStore/tables/ADSI_Table_1A_12.csv")
df=spark.read.schema(accident_schema).csv("/FileStore/tables/ADSI_Table_1A_12.csv")
display(df)

df.createOrReplaceTempView("accident_datasets")

spark.sql("SELECT State,count(State)FROM accident_datasets GROUP BY State").show()

#Calculating SUM, AVG , Visualisation of Total-Cause in ADSI_dataset.

df=df.withColumnRenamed("Total - Cases", "Total_accident_cause")
df.createOrReplaceTempView("accident_datasets")

Total_accident_cause = spark.sql(""" 
 SELECT State, SUM(Total_accident_cause) as Total_accident_cause
 FROM accident_datasets 
 GROUP BY State 
""") 
Total_accident_cause.show()

df=df.withColumnRenamed("Total - Cause", "Total_accident_cause")
df.createOrReplaceTempView("accident_datasets")
Total_accident_cause = spark.sql(""" 
 SELECT State, AVG(Total_accident_cause) as Total_accident_cause
 FROM accident_datasets
 GROUP BY State 
""") 
Total_accident_cause.show() 

import matplotlib.pyplot as plt
Total_accident_cause_pd= Total_accident_cause.toPandas()

plt.bar(Total_accident_cause_pd['State'],Total_accident_cause_pd['Total_accident_cause'])
plt.xlabel('State')
plt.ylabel('Total_accident_cause')
plt.xticks(rotation=90)
plt.title('Total Number of People Affected in Each State')
plt.show()


#Calculating SUM, AVG , Visualisation of Total-Injured in ADSI_dataset.

df=df.withColumnRenamed("Total - Injured", "Total_accident_injured")
df.createOrReplaceTempView("accident_datasets")

Total_accident_injured = spark.sql(""" 
 SELECT State, SUM(Total_accident_injured) as Total_accident_injured
 FROM accident_datasets 
 GROUP BY State 
""") 
Total_accident_injured.show() 

 
df=df.withColumnRenamed("Total - Injured", "Total_accident_injured")
df.createOrReplaceTempView("accident_datasets")
Total_accident_injured = spark.sql(""" 
 SELECT State, AVG(Total_accident_injured) as Total_accident_injured
 FROM accident_datasets
 GROUP BY State 
""") 
Total_accident_injured.show() 


import matplotlib.pyplot as plt
Total_accident_injured_pd= Total_accident_injured.toPandas()

plt.bar(Total_accident_injured_pd['State'],Total_accident_injured_pd['Total_accident_injured'])
plt.xlabel('State')
plt.ylabel('Total_accident_injured')
plt.xticks(rotation=90)
plt.title('Total Number of People injured in Each State')
plt.show()


#Calculating SUM, AVG , Visualisation of Total-Died in ADSI_dataset.

df=df.withColumnRenamed("Total - Died", "Total_accident_died")
df.createOrReplaceTempView("accident_datasets")

Total_accident_died = spark.sql(""" 
 SELECT State, SUM(Total_accident_died) as Total_accident_died
 FROM accident_datasets 
 GROUP BY State 
""") 
Total_accident_died.show() 



df=df.withColumnRenamed("Total - Died", "Total_accident_died")

df.createOrReplaceTempView("accident_datasets")

Total_accident_died = spark.sql(""" 
 SELECT State, AVG(Total_accident_died) as Total_accident_died
 FROM accident_datasets
 GROUP BY State 
""") 
Total_accident_died.show() 

import matplotlib.pyplot as plt
Total_accident_died_pd= Total_accident_died.toPandas()

plt.bar(Total_accident_died_pd['State'],Total_accident_died_pd['Total_accident_died'])
plt.xlabel('State')
plt.ylabel('Total_accident_died')
plt.xticks(rotation=90)
plt.title('Total Number of People died in Each State')
plt.show()










