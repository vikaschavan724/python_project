import sys
import os
from pyspark.sql.functions import *
"""os.environ['JAVA_HOME'] = "C:\Java\jre-1.8"
os.environ['SPARK_HOME'] = "C:\Spark\spark-3.4.1-bin-hadoop3"
os.environ['HADOOP_HOME'] = "C:\hadoop\hadoop-3.3.5" """
os.environ['PYSPARK_PYTHON'] = "python"
#sys.path.append("C:\Spark\spark-3.4.1-bin-hadoop3\python")
#sys.path.append("C:\Spark\spark-3.4.1-bin-hadoop3\python\lib")
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local").appName("Word Count").getOrCreate()
#print(spark)


data = [("Banana",1,"USA"),("Banana",2,"USA"), ("Carrots",4,"USA"), ("Beans",5,"USA")]
columns = ["Product","Amount","Country"]
df=spark.createDataFrame(data,columns)
#df.printSchema()
#df.show(truncate=False)
df.show()
df1=df.withColumn("Amount", ceil(col("Amount")/3))
df1.show()
df3=df.withColumn("Amount", floor(col("Amount")/3))
df3.show()
df2=df.withColumn("Amount", round(col("Amount")/3))
df2.show()

