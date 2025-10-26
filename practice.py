# install libraries with pip
#pip install wget pyspark  findspark

#import and init pyspark
import findspark
findspark.init()

#import pyspark context, session and config
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

#create context and session
sc = SparkContext.getOrCreate()

spark = SparkSession.builder.appName("Python Spark DataFrames basic example").config("spark.some.config.option", "some-value").getOrCreate()

#download datasets
import wget

dataset1 = wget.download('https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-BD0225EN-SkillsNetwork/labs/data/dataset1.csv')
dataset2 = wget.download('https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-BD0225EN-SkillsNetwork/labs/data/dataset2.csv')

#Create spark dataframes from datasets
df1 = spark.read.csv(dataset1, header=True, inferSchema=True)
df2 = spark.read.csv(dataset2, header=True, inferSchema=True)

#print schema
df1.printSchema()
df2.printSchema()

#Add a new column named year to df1 and quarter to df2 representing the year and quarter of the data.
df1 = df1.withColumn("year", year(to_date(col("date_column"),"dd/MM/yyyy")))
df2 = df2.withColumn("quarter", quarter(to_date(col("transaction_date"),"dd/MM/yyyy")))

#Rename the column amount to transaction_amount in `df1` and value to transaction_value in `df2`.
df1 = df1.withColumnRenamed("amount","transaction_amount")
df2 = df2.withColumnRenamed("value","transaction_value")

#Drop the columns description and location from df1 and notes from df2.
df1 = df1.drop(*["description","location"])
df2 = df2.drop("notes")

#Join df1 and df2 based on the common column customer_id and create a new dataframe named joined_df.
joined_df = df1.join(df2, on="customer_id", how="inner")

#Filter joined_df to include only transactions where "transaction_amount" is greater than 1000 and create a new dataframe named filtered_df.
filtered_df = joined_df.filter(joined_df["transaction_amount"] > 1000)

#Calculate the total transaction amount for each customer in filtered_df and display the result.
grouped_df = filtered_df.groupBy("customer_id").agg({"transaction_amount":"sum"}).withColumnRenamed("sum(transaction_amount)","total_transaction_amount")

#Write total_amount_per_customer to a Hive table named customer_totals.
grouped_df.write.mode("overwrite").saveAsTable("customer_totals")

#Write filtered_df to HDFS in parquet format to a file named filtered_data.
filtered_df.write.mode("overwrite").parquet("filtered_data.parquet")

#Add a new column named high_value to df1 indicating whether the transaction_amount is greater than 5000. When the value is greater than 5000, the value of the column should be Yes. When the value is less than or equal to 5000, the value of the column should be No.
df1 = df1.withColumn("high_value", when(col("transaction_amount") > 5000, lit("Yes")).otherwise("No"))

#Calculate and display the average transaction value for each quarter in `df2` and create a new dataframe named `average_value_per_quarter` with column `avg_trans_val`.
average_value_per_quarter = df2.groupBy("quarter").agg({"transaction_value":"AVG"}).withColumnRenamed("avg(transaction_value)","avg_trans_val")

#Write average_value_per_quarter to a Hive table named quarterly_averages.
average_value_per_quarter.write.mode("overwrite").saveAsTable("quarterly_averages")

#Calculate and display the total transaction value for each year in df1 and create a new dataframe named total_value_per_year with column total_transaction_val.
total_value_per_year = df1.groupBy("year").agg({"transaction_amount":"sum"}).withColumnRenamed("sum(transaction_amount)","total_transaction_val")

#Write total_value_per_year to HDFS in the CSV format to file named total_value_per_year.
total_value_per_year.write.mode("overwrite").csv("total_value_per_year.csv")
