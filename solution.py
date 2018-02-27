#To decrypt gpg encoded csv file
# % gpg --import slim.shady.sec.asc
# % gpg --decrypt titanic.csv.gpg  > titanic.csv


from pyspark.sql import SQLContext
from pyspark.sql.types import *

sqlContext = SQLContext(sc)

df = sqlContext.read.load('titanic.csv', 
                      format='com.databricks.spark.csv', 
                      header='true', 
                      inferSchema='true')
df = df.withColumn("Age", df["Age"].cast(DoubleType()))
avg_age = df.agg({"age": "avg"}).collect()[0]
print "average age: %f" % avg_age
#average age: 30.397989

df.registerTempTable("df")
percentile = sqlContext.sql("select percentile_approx(age,0.75) as approxQuantile from df").collect()[0]
print "75th percentile: %f" %percentile
#75th percentile: 38.857143