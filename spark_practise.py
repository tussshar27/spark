# Spark is an open source unified computing engine with a set of libraries for parallel data processing on computer cluster.
# it is 100x faster than hadoop mapreduce because it process data in RAM instead of disk which hadoop uses.
# spark is built on scala.


spark components: low level
               +-----------------------+
               |     Spark Streaming    |
               +-----------------------+
               |        Spark SQL       |
               +-----------------------+
               |         MLlib          |
               +-----------------------+
               |         GraphX         |
               +-----------------------+
               |       Spark Core       |
               +-----------------------+

high level:
Libraries , Structured streaming and advanced analytics
structured API - DataFrames, Datasets and SQL
low level API - RDD and distributed variables

to create spark session:
# Spark Session
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = (
    SparkSession
    .builder
    .appName("Spark Introduction")
    .master("local[*]")
    .getOrCreate()
)

#command to check session is created
spark

#to create a dataframe
emp_data = [
    ["001","101","John Doe","30","Male","50000","2015-01-01"],
    ["002","101","Jane Smith","25","Female","45000","2016-02-15"],
    ["003","102","Bob Brown","35","Male","55000","2014-05-01"],
    ["004","102","Alice Lee","28","Female","48000","2017-09-30"],
    ["005","103","Jack Chan","40","Male","60000","2013-04-01"]
]

emp_schema = "employee_id string, department_id string, name string, age string, gender string, salary string, hire_date string"

emp = spark.createDataFrame(data=emp_data, schema=emp_schema)
emp.show()

# Write our first Transformation (EMP salary > 50000)
emp_final = emp.where("salary > 50000")

#to check the number of partitions created
emp.rdd.getNumPartitions()

# Write data as CSV output (ACTION)
emp_final.write.format("csv").save("data/output/1/emp.csv")

# Schema for emp
emp.schema
#output:
StructType([StructField('employee_id', StringType(), True), StructField('department_id', StringType(), True), StructField('name', StringType(), True), StructField('age', StringType(), True), StructField('gender', StringType(), True), StructField('salary', StringType(), True), StructField('hire_date', StringType(), True)])

emp.printSchema()
#output:
root
 |-- emp_id: string (nullable = true)
 |-- name: string (nullable = true)
 |-- age: integer (nullable = true)
 |-- salary: string (nullable = true)

#creating small dataframe
# Small Example for Schema
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
schema_string = "name string, age int"

schema_spark =  StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)
])

#select on dataframe:
		 
# SELECT columns
# select employee_id, name, age, salary from emp


# Filter emp based on Age > 30
# select emp_id, name, age, salary from emp_casted where age > 30
emp_final = emp_casted.select("emp_id", "name", "age", "salary").where("age > 30")
		 
#expr()
emp_filtered = emp.select(col("employee_id"), expr("name"), emp.age, emp.salary)		#The expr() function in Spark (from pyspark.sql.functions) lets you use SQL expressions directly in DataFrame APIs.
emp_filtered.show()

#selectExpr()
emp_casted_1 = emp_filtered.selectExpr("employee_id as emp_id", "name", "cast(age as int) as age", "salary")
emp_casted_1.show()

#Bonus Tip:
#spark has a built-in function which converts your basic schema string to the spark native datatype.
schema_string = "name string, age int"
from pyspark.sql.types import _parse_datatype_string
df_sch_str = _parse_datatype_string(df_sch_str)
df_sch_str

#output:
StructType([StructField('name', StringType(), True), StructField('age', IntegerType, True)]), True)]), True)]) 
																 
#cast() using spark dataframe API
from pyspark.sql.functions import col, cast
df.select(col("emp_id").cast("int"),"name","age",col("salary").cast("double"))
df.printSchema()

#create multiple new columns or overwrite existing column
#old method
df = df.withColumn("tax",col("salary")*0.2).withColumn("pension",col("salary")*0.1)
df.show()

#new method for spark >= 3.4
#NOTE: if your code is inside parentheses, brackets, or braces, Python automatically allows implicit line continuation — no backslash is needed.
df = df.withColumns({"tax":col("salary")*0.2, 
					 "pension":col("salary")*0.1})

#static value new column
from pyspark.sql.functions import lit
df = df.withColumn("flag",lit("Y"))
df.show()

#rename a column
df = df.withColumnRenamed("employee_id","emp_id")
df.show()

#renaming using expr() or selectExpr(), these both are transformations, not actions
df.select("id","name",expr("employee_id as emp_id")).show()
df.selectExpr("id","name","employee_id as emp_id").show()

#removing multiple existing column
df = df.drop("column1","column2")
df.show()

#filter data
df = df.filter("salary > 10000")
df.show()

#LIMIT data
df = df.limit(5)	#if we want to write data.
df.show()
OR
df.show(5)	#directly do it in console without writing data.

#case when statement
from pyspark.sql.functions import when, col
#NOTE: otherwise() is not imported b/c it is chained to when()
#pyspark follows python syntax that's why == is used instead of = .
#also \ is not used as a line breaker b/c withColumn() is opened.
#in py, None is given instead of null but in output it will show null.
df = df.withColumn("new_gender",when(col("gender")=="Male","M")
				  .when(col("gender")=="Female","F")
				  .otherwise("None")
				  )
df.show()
#OR
df = df.withColumn("new_gender",expr("case when gender = 'Male' then 'M' when gender = 'Female' then 'F' else null end"))
df.show()

#regexp_replace()
#in sql, SELECT name, REPLACE(name, 'J', 'Z') AS new_name FROM employee;
from pyspark.sql.functions import regexp_replace
df = df.withColumn("new_name",regexp_replace(col("name"),'J','Z'))
df.show()

#convert string datatype to date
from pyspark.sql.functions import col, to_date
df = df.withColumn("new_hire",to_date(col("new_hire"),"yyyy-MM-dd"))
df.printSchema()

#create new columns having current data and timestamp
from pyspark.sql.functions import current_date, current_timestamp
df = df.withColumn("current_date", current_date())\
		.withColumn("current_timestamp", current_timestamp())
df.show(truncate=False)

#convert date to string
# there are hundreds of abbreviation to use with date_format() function. refer spark documentation for more info.
from pyspark.sql.functions import date_format
df = df.withColumn("date_string",date_format(col("hire_date"),"dd/MM/yyyy"))\
		.withColumn("date_year",date_format(col("hire_date"),"yyyy"))\
		.withColumn("date_month",date_format(col("hire_date"),"MM"))
#same we can do for timestamp, refer spark doc in order to get hour, minute or second.
df.show()


#removing records having any null value
df = df.na.drop()
#OR
df = df.dropna()

#Drop rows only if all columns are null
df = df.dropna(how="all")

#Drop rows based on specific columns
df_drop_subset = df.dropna(subset=["name"])

#Threshold-based drop (minimum non-null columns required)
df_drop_thresh = df.dropna(thresh=2)

#fixing null values
#in prod, we can't drop any records.
from pyspark.sql.functions import col, lit, coalesce
df = df.withColumn("new_gender",coalesce(col("gender"),lit("O")))

#drop old columns and fix new columns
#The reason withColumn() or withColumnRenamed() is not imported from pyspark.sql.functions is because it is a method of the DataFrame class, not a standalone Spark SQL function.
df = df.drop("name","gender")\
		.withColumnRenamed("new_name","name")\
		.withColumnRenamed("new_gender","gender")
df.show()

#write final data to csv
df.write.format("csv").save("/data/output/file.csv")

#UNION and UNION ALL
#all the column names, its datatype and its column sequence should be same to perform.
#union remove the duplicates whereas union all does not.
#union:
df = df_1.union(df_2)
df.show()

#unionAll:
df = df_1.unionAll(df_2)
df.show()

#sorting
from pyspark.sql.functions import asc, desc, col
df = df.orderBy(col("salary").desc())
df.show()
df = df.orderBy(col("salary").asc())
df.show()

#aggregation:
#SQL: select dept_id, count(emp_id) as emp_count from employee group by dept_id;
#NOTE: not importing groupBy() and agg() b/c they are based on dataframe objects and not based on sql functions.

from pyspark.sql.functions import count, sum

df = df.groupBy("dept_id") \
       .agg(
           count("emp_id").alias("emp_count"),
           sum("salary").alias("sal_sum")
       )

df.show()












df.explain(extended=True)

dbutils.fs.ls(/FileStore/tables/)

df = spark.read.format('csv')\
	.option('header','true')\
	.option('inferSchema','true')\
	.option('mode','PERMISSIVE')\
	.option('nullValue','NA')\
	.option('delimiter','|')\
	.option('badRecordsPath','/mnt/badRecords/')\
	.load('dbfs:/FileStore/tables/file1.csv')
	
df.repartition(10).write.format('parquet')\
	.option('mode','append')\
	.save('/FileStore/tables/File1')
	
	
print(df.rdd.getNumPartitions())

df.write.format('delta')\
	.option('mode','overwrite')\
	.saveAsTable('default.table1')
	

df.dropDuplicates(['id','name']).show()

df = df.filter(col('id').isin(10,20,30))

df.filter(col('id').isNotNull()).select('*').show()

df = df.fillna({'id':0})

df = df.groupBy(col('deptid').desc()).agg(sum(col('sales')).alias('sum_of_sales'),max(col('age').alias('max_age')))

df = df.withColumn('fic_mis_date',to_date(col('fic_mis_date')))

from pyspark.sql.window import Window
window_spec = Window.partitionBy(col('deptid')).orderBy(col('salary').desc())
df = df.withColumn('rnk',row_number().over(window_spec)).filter(col('rnk')==1)
df.show()

df = df.withColumn('current_date',current_date())\
		.withColumn('day_after_30',date_add('current_date',30))\
		.withColumn('day_before_30',date_sub('current_date',30))\
		.withColumn('months_bet',months_between(current_date(),to_date('01-01-2025')))
		
df = df.withColumn('feedback',split(col('response'))[0])

from pyspark.sql.window import Window
window_spec = Window.partitionBy(col('category')).orderBy(col('sales').desc())
df = df.withColumn('running_sales',sum(col('sales')).over(window_spec))

df = df.withColumn('month',month(col('date')))\
		.withColumn('year',year(col('date')))
		
df = df.withColumn('status',when(col('flag')==1,'Yes').when(col('flag)==0,'No').otherwise(None))

#to create table from dataframe 
df.createOrReplaceGlobalTempView('table1')
spark.sql("select * from table1")

#to create dataframe from table 
df_tble1 = spark.table('table1')

df.select('id','name','salary','deptname').createOrReplaceGlobalTempView('table2')


df = spark.read.format('json').option('mergeSchema','true').option('mode','PERMISSIVE').load('path/to/load')
df.write.format('delta').option('mode','append').option('mergeSchema'.'true').save('path/to/existing/write')

# OPTIMIIZE delta_table			..will coalesce partitions

# OPTIMIZE delta_table ZORDER BY (order_date)		..will coalesce partitions and sort the data inside it.

# string_agg in MSSQL:
# collect_list in pyspark:
df.groupBy(col('deptid')).agg(collect_list(col('name')).alias('name_list'))

df.groupBy(col('deptid')).agg(collect_set(col('name')).alias('name_list'))			#removes duplicates

df = df.withColumn('fullname',concat_ws(' ',col('firstname'),col('lastname')))

df.filter(col('phone').like('91%'))

df = df.withColumn('status',when(length(col('name'))==5,'Yes').otherwise('No'))

df_existing = spark.createDataFrame(data,column)
df_existing.write.format('delta')\
	.option('mode','append')\
	.load('path')
df_update = spark.createDataFrame(data,column)
delta_table = DeltaTable.forPath(spark,'path')
delta_table.alias('target').merge(df_update.alias('source'),'id = id')\
							.whenMatchedUpdate({'name':'source.name','age':'source:age'})\
							.whenNotMatchedInsert({'id':'source.id','name':'source.name'})\
							.execute()
spark.read.format('delta').load('path').show()

create schema db.raw
create table db.raw.managed_table(
id int null,
name string
)
using DELTA;
# NOTE: everything is stored in form of files.

create table db.raw.ext_table(
id int null,
name string
)
using DELTA
LOCATION 'abfss://raw@storageaccount/ext_table';		..just by giving location, we can create external table.

%sql
select * from delta.'path'

# -- Shallow Clone
CREATE TABLE new_table_name SHALLOW CLONE source_table_name;

# -- Deep Clone
CREATE TABLE new_table_name DEEP CLONE source_table_name;

# -- what are transformations?
# transformations are operations on existing RDDs/dataframes that produce new RDDs/dataframes.

use of parallelize(), map(), filter(), flatmap() and reduce() in spark:
1. parallelize(): it is used to create a new rdd from a list in python or scala.
eg: sc.parallelize([1,2,3,4])

2. map(): it is a transformation that applies a function to each element of a RDD and returns a new RDD.

3. filter(): it is another transformation that selects elements from the RDD that satisfies the function.

3. flatMap(): it is also a transformation which is similar to map() just that it is used to merge sublist to product single RDD list.
so it is a combination of both map and flatten.

4. reduce(): it is an action which aggregates all the elements of an RDD and produce a single output.


example of all three functions: if you have a list in your driver program, say [1,2,3,4,5], and you parallelize it into an RDD. 
Then you can apply a map to double each number, then filter to keep numbers greater than 5. The result would be [6,8,10].

data = [1,2,3,4]

rdd = sc.parallelize(data,numSlices=2)	#sets the number of partitions

mapped = rdd.map(lambda x:x**2)	#[1,2,9,16]

filtered = mapped.filter(lambda x:x>4)	#[9,16]

flatmapped = filtered.flatMap(lambda x:[x,x*2])	#[9,18,16,32]

#for subtraction, the value will be invalid because it does operation by doing shuffling with data with partitions.
total = flatmapped.reduce(lambda a,b: a+b)	#(9+18+16+32)	#aggregates the data with shuffling

print(total)

map in python: takes two arguments i.e. list(map(expression,listdata))
map in rdd: takes only one argument i.e. rdd.map(expression)

difference between reduceByKey() and groupByKey(): both are used with key-value pairs in RDDs
groupByKey(): it groups the values having same key. it shuffles the data across the partitions.
eg: data = [(A,1), (B,2), (A,3), (C,4)]
rdd = sc.parallelize(data)
grouped = rdd.groupByKey()
print(grouped)
#[('A',[1,2]), ('B',[3]), ('C',[4])]

reduceByKey(): it aggregates the data within partition before doing shuffling.
eg: data = [(A,1), (B,2), (A,3), (C,4)]
rdd = sc.parallelize(data)
reduced = rdd.reduceByKey(lambda a,b: a+b)
print(reduced)
#[('A',4),('B',2),('C',4)]

also, groupbykey gives grouped output where as reducebykey gives aggregated output.
so reduceByKey() is more efficient than groupByKey()


