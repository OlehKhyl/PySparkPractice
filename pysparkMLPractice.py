# Import findspark and init to work with pyspark
import findspark
findspark.init()

# Import libraries
from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegression
from pyspark.ml import Pipeline
from pyspark.ml.pipeline import PipelineModel
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StandardScaler

#create spark session
spark = SparkSession.builder.appName("MPG Regression").getOrCreate()

#Load file wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-BD0231EN-Coursera/datasets/mpg-raw.csv

#read CSV with spark into spark DataFrame
df = spark.read.csv("mpg-raw.csv", header=True, inferSchema=True)

#check top entries
df.head(5)

#Clean data from dublicates and missing values
df = df.dropDuplicates()
df = df.dropna()

#Rename column
df = df.withColumnRenamed("Engine Disp", "Engine_Disp")

#Save data as parquet file
df.write.parquet("mpg-cleaned.parquet")

#Load data from parquet file
df = spark.read.parquet("mpg-cleaned.parquet")

#Use indexer to convert categorical column into int
indexer = StringIndexer(inputCol="Origin", outputCol="OriginIndex")

#Use VectorAssembler to combine features into vector that can be used in regression
assembler = VectorAssembler(inputCols=['Cylinders', 'Engine_Disp', 'Horsepower', 'Weight', 'Accelerate', 'Year'], outputCol="features")

#Use Scaler to normalize values
scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures")

#define a regression, features is what we show to model, and label is what it must predict
lr = LinearRegression(featuresCol="scaledFeatures", labelCol="MPG")

#combine stages into pipeline
pipeline = Pipeline(stages=[indexer,assembler,scaler,lr])

#Split data training part for fit model and testing part for evaluation model
(trainingData, testingData) = df.randomSplit([0.7,0.3], seed=42)

#fit model
pipelineModel = pipeline.fit(trainingData)

#use model to make prediction on test data
predictions = pipelineModel.transform(testingData)

#evaluate model
from pyspark.ml.evaluation import RegressionEvaluator

evaluator = RegressionEvaluator(labelCol="MPG", predictionCol="prediction", metricName="mse")
mse = evaluator.evaluate(predictions)
print(mse)

evaluator = RegressionEvaluator(labelCol="MPG", predictionCol="prediction", metricName="mae")
mae = evaluator.evaluate(predictions)
print(mae)

evaluator = RegressionEvaluator(labelCol="MPG", predictionCol="prediction", metricName="r2")
r2 = evaluator.evaluate(predictions)
print(r2)

#save model in case we need use it later or send someone
pipelineModel.write().overwrite().save("./Practice_Project/")

#load model
loadedPipeline = PipelineModel.load("./Practice_Project/")

#check is it working
predictions_load_model = loadedPipeline.transform(testingData)
predictions_load_model.head(5)

#stop spark session
spark.stop()
