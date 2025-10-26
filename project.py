# Installing required packages  

# !pip install pyspark  findspark wget

import findspark
findspark.init()
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

sc = SparkContext.getOrCreate()

spark = SparkSession.builder.appName("Python Spark DataFrames basic example").config("spark.some.config.option", "some-value").getOrCreate()

import wget
employees_csv = wget.download("https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-BD0225EN-SkillsNetwork/data/employees.csv")

#define user schema and load data from csv using schema
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
schema = StructType([
    StructField("Emp_No", IntegerType(), False),
    StructField("Emp_Name", StringType(), False),
    StructField("Salary", IntegerType(), True),
    StructField("Age", IntegerType(), True),
    StructField("Department", StringType(),True)
])

employees_df = spark.read.csv(employees_csv, header=True, schema=schema)

# Create a temporary view named "employees" for the DataFrame
employees_df.createTempView("employees")

# SQL query to fetch solely the records from the View where the age exceeds 30
spark.sql("SELECT * FROM employees WHERE Age > 30").show()

# SQL query to calculate the average salary of employees grouped by department
spark.sql("SELECT Department, AVG(Salary) as avg_salary FROM employees GROUP BY Department").show()

# Apply a filter to select records where the department is 'IT'
spark.sql("SELECT * FROM employees WHERE Department = 'IT'").show()


# Add a new column "SalaryAfterBonus" with 10% bonus added to the original salary
from pyspark.sql.functions import col

employees_df = employees_df.withColumn("SalaryAfterBonus", col("Salary") + (col("Salary")*0.1))
employees_df.createOrReplaceTempView("employees")

# Group data by age and calculate the maximum salary for each age group
spark.sql("SELECT Age, MAX(Salary) AS Max_salary FROM employees GROUP BY Age").show()

# Join the DataFrame with itself based on the "Emp_No" column
spark.sql("SELECT t1.* FROM employees AS t1 JOIN employees AS t2 ON t2.Emp_No = t1.Emp_No").show()

# Calculate the average age of employees
spark.sql("SELECT avg(Age) as AVG_AGE FROM employees").show()

# Calculate the total salary for each department.
from pyspark.sql.functions import sum 

employees_df.groupBy("Department").agg(sum("Salary").alias("sum_salary")).show()

# Sort the DataFrame by age in ascending order and then by salary in descending order
from pyspark.sql.functions import asc, desc
employees_df.sort("Age", desc("Salary")).show()

# Calculate the number of employees in each department
from pyspark.sql.functions import count

employees_df.groupBy("Department").agg(count("Emp_No").alias("CNT")).show()

# Apply a filter to select records where the employee's name contains the letter 'o'
employees_df.filter(col("Emp_Name").contains("o")).show()
