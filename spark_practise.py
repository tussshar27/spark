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

#scenario: what if the two dataframes have same columns with same datatypes but the column sequence is different?
#unionByName()
df = df_1.unionByName(df_2)
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

#HAVING clause
#SQL: select dept_id, count(emp_id) as emp_count form employee group by dept_id having count(emp_id) > 2 ;
from pyspark.sql.functions import count, col
df = df.groupBy(col("dept_id")).agg(count(col("emp_id")).alias("emp_count")).filter("emp_count > 2")
df.show()

#BONUS TIP
#spark also provides count() function to get the count of records of a dataframe directly
df.count()

#get unique data from dataframe
#SQL: select distinct emp.* from emp;

df = df.distinct()			#performing distinct() on all columns of dataframe
df.show()
#to get distinct department id
df = df.select("dept_id").distinct()
df.show()

#col(): col() function is used when we perform transformation or use filter on top of that column.

#analytical window functions

from pyspark.sql.window import Window
from pyspark.sql.functions import col, max, desc, row_number

#Eg1.
#SQL: select dept_id, max(salary) over(partition by dept_id) as max_sal from emp;
window_spec = Window.partitionBy(col("dept_id"))
df = df.withColumn("max_salary",max(col("salary")).over(window_spec))
df.show()

#Eg2.
#SQL: select e.* from (select dept_id, row_number() over(partition by dept_id order by salary) as emp_rank from emp) e where e.emp_rank = 1;
window_spec = Window.partitionBy(col("dept_id")).orderBy(col("salary").desc())
df = df.withColumn("emp_rank",row_number().over(window_spec))
df = df.filter(col("emp_rank") == 1)					#same as df = df.filter("emp_rank == 1")
df.show()

#using expr
from pyspark.sql.functions import expr
df = df.withColumn("max_salary", expr("row_number() over(partition by dept_id order by salary desc)")).filter("max_salary == 2")))
df.show()

#to know the number of partitions our dataframe has:
print(df.rdd.getNumPartitions())		#OR 		df.rdd.getNumPartitions()
#output:
8

#REPARTITION (increase or decrease partitions) and COALESCE (decrease partitions):
#repartition: data shuffling happens, guarantees uniform distribution
#Eg1. reducing the number of partitions to 4
df = df.repartition(4)
df.rdd.getNumPartitions()
#output:
4

#Eg1. increasing the number of partitions to 100
df = df.repartition(100)
df.rdd.getNumPartitions()
#output:
100

#repartition data based on columns
df = df.repartition(4, "dept_id")		#data is partitioned based on dept_id
df.getNumPartitions()
#output:
4


#coalesce: no data shuffling happens , and can't guarantee uniform data distribution
#if the current partitions are 8 and you do 100 then also it will show 8 b/c it can't increase the partitions.
df.getNumPartitions()
df = df.coalesce(100)
df.getNumPartitions()
8

#but we can decrease the number of partitions
df = df.coalesce(3)
df.getNumPartitions()
3

#how repartition works under the hood?
1. spark uses hash partitioning strategy to perform repartition with columns.
2. for each row, it computes hash of the partition key.
3. each partition has its own index [0 ... numPartitions-1]
4. repartition maps thath hash to its partition index by using the formula: partition_index = nonNegativeHash(hash) % numPartitions Eg. 2 = 2 % 4
5. all rows whose key hashes to the same index are placed into same partition

#How to inspect which row went to which partition?
1. by using spark_partition_id() we can find each row's partition index.
2. Eg. creating a new column to get the partition index of each row whicle performing repartition:
from pyspark.sql.functions import spark_partition_id
df = df.repartition(4,"dept_id").withColumn("partition",spark_partition_id())
df.show()
3. Eg. getting the distribution count of rows in each partition or to check partition Skew:
from pyspark.sql.functions import col, count, spark_partition_id
df.groupBy(col("partition")).count().show()	OR df.groupBy(spark_partition_id()).count().show()
4. to check which dept_id value went to which partition:
df.select("dept_id","partition").distinct().orderBy(col("partition").asc()).show(truncate=False)

#practical usecase: repartition
when we perform df.withColumn("partition",spark_partition_id()).show() , we will see some of the dept_id are split into different partitions
before repartition:
Eg.
dept_id		partition
1				1
1				1
2				2
3				3
4				3
1				3
1				2

we can see that dept_id = 1 is distributed in various partitions
so to make it locate in same partition , we will use repartition which allocate same partitions for same dept_id using hash function
df = df.repartition(3,"dept_id")
df.show()
after repartition:
Eg.
dept_id		partition
1				1
1				1
2				2
3				3
4				3
1				1
1				1

#INNER JOIN:
#SQL: select emp_id, dept_id, salary from emp inner join dept on emp.dept_id = dept.dept_id ;
df = emp.join(dept, how="inner", on=emp.dept_id==dept.dept_id)	OR	df = emp.alias("e").join(dept.alias("d"), how="inner", on=emp.dept_id==dept.dept_id)
df.select("e.emp_id", "d.dept_id", "e.salary").show()		#put columns in "" when we use alias

#LEFT OUTER JOIN:
#SQL: select emp_id, dept_id, salary from emp left outer join dept on emp.dept_id = dept.dept_id ;
df = emp.join(dept, how="left_outer", on=emp.dept_id==dept.dept_id)	OR	df = emp.alias("e").join(dept.alias("d"), how="left_outer", on=emp.dept_id==dept.dept_id)
df.select("e.emp_id", "d.dept_id", "e.salary").show()		#put columns in "" when we use alias

#ADVANCED CASCADING JOIN:
1. join with cascading condition with dept_id and only for department 101 and 102 and not null/null conditions.
df = emp.join(
    dept,
    how="left_outer",
    on=(emp.dept_id == dept.dept_id) & 
       ((dept.dept_id == "101") | (dept.dept_id == "102")) & 
       (dept.dept_id.isNotNull())
)

#SPARK DEEP-DIVE
#CREATE SPARK SESSION:
from pyspark.sql import SparkSession		#importing SparkSession class
spark = (
	SparkSession
	.builder								#create interface for SparkSession
	.appName("Reading from CSV files")		#sets the name of your spark application , will be appear in spark UI, logs
	.master("local[*]")						#it says to run spark locally with all the available CPU cores. local[2]-> to use only 2 cores locally. local not used for PROD
	.getOrCreate()							#Returns an existing SparkSession if one already exists, otherwise creates a new one
)


#Before Spark 2.0, you had to create a SparkContext and then wrap it with a SQLContext or HiveContext:
from pyspark import SparkContext
from pyspark.sql import SQLContext
sc = SparkContext()
sqlContext = SQLContext(sc)

#from above examples
#what is SparkSession?
1. SparkSession is a unified entry point introduced in Spark 2.0 that encapsulates the SparkContext and SQLContext. 
2. It allows you to interact with both the lower-level RDD API and the higher-level DataFrame and SQL APIs.


from pyspark.sql import SparkSession
spark = (
	SparkSession	#class
	.builder		#initialize
	.appName("My Spark Application")	#give name to session
	.master("local[*]")					#inclusde all available CPU cores
	.getOrCreate()				#create new session if it doesn't exist.
)
spark
#spark UI link will be appeared in output once we run above code.

#inside Spark UI:
Job link > Stage link > task link
https://youtu.be/k2AVOmUS7i0?si=U8PAyr1JgI1m8bkX

Tabs inside Spark UI:
Jobs	 Stages     Storage		Environment		Executors	  SQL/Dataframe

#to read CSV data:
df = spark.read.format("csv")\
	.option("header",True)\
	.load("/data/input/emp.csv")

df.printSchema()					#printSchema is not an action so it won't trigger spark job as it displays only the schema of metadata

#jobs are created when an action is called.
#but here spark creates job while reading above csv file even there is no action because spark proactively creates job in order to identify the metadata/header of a file.

#if we read file without giving option header then spark will create 1 job > 1 stage > 1 task and in that task it will read 1 record which we can see from spark UI

df = spark.read.format("csv")\
	.option("header",True)\
	.option("inferSchema",True)\		#spark automatically identifies datatype of each column
	.load("/data/input/emp.csv")
# if we run with inferSchema as True while reading file then spark will create 2 jobs > 1 stage each > 1 task each and 1 task will read one record and the other task will read all the records including header (eg. 20 records + 1 header).


# now consider a case were we pre define schema.
#then spark does not need to go and check for schema in the file.

_schema = "emp_id int, dept_id int, name string, age int, gender string, salary double, hire_date date"
df = spark.read.format("csv").option("header",True).schema("_schema").load("/data/input/emp.csv")

#IMPORTANT:
#now if we run above two commands, spark won't create any job because we have pre defined schema for spark so that spark won't check the metadata from the file itself.
#and because if it spark won't create a job to read the file's schema.
#that's why it is very important to define schema in production before reading the file, this will help spark to optimize the code.

df.show()

#NOTE: inferSchema does not read all the records — it only reads a sample of rows from the dataset to infer the schema (column data types).

#ways to define schema in spark:
1. using inferSchema():
df = spark.read.format("csv").option("header",True).schema("_schema").load("/data/input/emp.csv")
#spark reads schema automatically, performance overhead.

2. using StructType() and StructField():(Recommended)
from pyspark.sql.types import StructType , StructField, IntegerType
_schema = StructType([
	StructField("emp_id", IntegerType(), True),
	StructField("dept_id", IntegerType(), True"),
	StructField("name", StringType(), True),
	StructField("salary", DoubleType(), True)
])

df = spark.read.format("csv").option("header",True).schema(_schema).load("/data/input/data1.csv")

3. using SQL like statement: (less flexibility as we can't define if any column can take default null values)
_schema = "emp_id int, dept_id int, name string, salary double"
df = spark.read.format("csv").option("header",True).schema(_schema).load("data1.csv")
df.show()

#Mode:
#mode in spark is useful to handle bad records.
#we need to apply schema in order to use mode, we cant do it using inferSchema.
#spark's default mode in PERMMISSIVE.
there are three types of modes:
1. PERMISSIVE (default mode): it puts bad records into different column i.e. _corrupt_record.
Eg. not mandatory to write .option("mode", "PERMISSIVE")
_schema = "emp_id int, dept_id int, name string, salary double"
df = spark.read.format("csv").option("mode", "PERMISSIVE").option("header",True).schema(_schema).load("data1.csv")

#if we run abve commands, it will not reject corrupt records , it will put Null inplace of it.
#but how do we identify which column data has corrupt values?
#for that spark provides default column which we need to call while defining schema

input data:
emp_id,dept_id,name,salary
1,101,Tushar,50000
2,102,Annam,60000
3,101,John,abc
4,David,70000
5,103,Robert,65000

_schema = "emp_id int, dept_id int, name string, salary double,		 _corrupt_record string"
df = spark.read.format("csv").option("mode", "PERMISSIVE").option("header",True).schema(_schema).load("data1.csv")

output data:
+-------+--------+-------+------+----------------------+
|emp_id |dept_id |name   |salary|_corrupt_record       |
+-------+--------+-------+------+----------------------+
|1      |101     |Tushar |50000 |null                  |
|2      |102     |Annam  |60000 |null                  |
|3      |101     |John   |null  |null                  |
|4      |null    |null   |null  |4,David,70000         |	#corrupt data populated, full row gets populated in _corrupt_record column.
|5      |103     |Robert |65000 |null                  |
+-------+--------+-------+------+----------------------+

from pyspark.sql.functions import col
df.filter(col("_corrupt_record").isNotNull()).show()		#OR		df.where("_corrupt_record is not null").show()

#NOTE: it is not mandatory to use _corrupt_record as a column name. we can give different name using columnNameOfCorruptRecord option.
_schema = "emp_id int, dept_id int, name string, salary double,		 bad_record string"
df = spark.read.format("csv").option("header",True).option("mode", "PERMISSIVE").option(columnNameOfCorruptRecord,"bad_record").schema(_schema).load("data1.csv")
+-------+--------+-------+------+----------------+
|emp_id |dept_id |name   |salary|bad_record      |
+-------+--------+-------+------+----------------+
|1      |101     |Tushar |50000 |null            |
|2      |102     |Annam  |60000 |null            |
|3      |101     |John   |null  |null            |
|4      |null    |null   |null  |4,David,70000   |
|5      |103     |Robert |65000 |null            |
+-------+--------+-------+------+----------------+


#using StructType() and StructField() functions for schema defination:
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

schema = StructType([
    StructField("emp_id", IntegerType(), True),
    StructField("dept_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("salary", DoubleType(), True)
])

df = (
    spark.read
    .option("header", "true")
    .option("mode", "PERMISSIVE")
    .option("columnNameOfCorruptRecord", "bad_record")
    .schema(schema)
    .csv("employees.csv")
)

#Spark will automatically add a bad_record column if any row doesn’t match the schema.
StructField("_corrupt_record", StringType(), True)
then Spark will not treat it as a special column anymore.
It will just treat it like a normal string column —
so malformed rows won’t go there automatically.

So you’ll lose the automatic malformed record tracking behavior.

2. DROPMALFORMED:
#it drops the bad records.
_schema = "emp_id int, dept_id int, name string, salary double"
df = spark.read.format("csv").option("mode","DROPMALFORMED").option("header", True).schema("_schema").load("data.csv")
df.show()
#IT WILL REMOVE ALL THE CORRUPT RECORD ROWS AND SHOW ONLY THE VALID RECORD ROWS.


3. FAILFAST:
#it fails when there is any bad records.
_schema = "emp_id int, dept_id int, name string, salary double"
df = spark.read.format("csv").option("mode","FAILFAST").option("header", True).schema("_schema").load("data.csv")
df.show()

#BONUS TIP:
#we are writing .option() multiple times but there is one alternative to it using dictionary.
_options = {
	"header":"true",
	"inferschema":"true",
	"mode":"PERMISSIVE"
}
df = spark.read.format("csv").option(**_options).load("data.csv")
df.show()

#row format and columnar format:
#sample data:
| EMP_ID | EMP_NAME | SALARY |
| ------ | -------- | ------ |
| A001   | DEXTER   | 500    |
| A002   | TOM      | 600    |
| A003   | JERRY    | 1000   |

#row format:
A001,DEXTER,500,A002,TOM,600,A003,JERRY,1000

#columnar format:
A001,A002,A003,DEXTER,TOM,JERRY,500,600,1000

Example: consider we need to find aggregatikon of salary.
in case of row format, the system has to go from each column record in order to access salary column value of each row.
but in columnar format, the system has to just read only the last column values which has all the salary value.
so this is how columnar format can provide you a lot of optimization processing benefits when you want to do data analysis.

#Comparison: PARQUET vs ORC vs AVRO
| PROPERTY / FILE FORMAT | PARQUET            | ORC        | AVRO  |
| ---------------------- | ------------------ | ---------- | ----- |
| **COMPRESSION**        | Better             | Best       | Good  |
| **READ / WRITE**       | Read               | Heavy Read | Write |
| **ROW / COLUMNAR**     | Column             | Column     | Row   |
| **SCHEMA EVOLUTION**   | Good               | Better     | Best  |
| **EXAMPLE USE**        | Delta Lake / Spark | Hive       | Kafka |

Recommended file format:
1. for Spark / Delta Lake : PARQUET
2. for Hive : ORC
3. for Kafka : AVRO
#IMPORTANT:
#in columnar data format, if you read the column for data analysis then it will be beneficial.
eg.
	count(column1) >>> count()

#NOTE: the default configuration of parquet and ORC is snappy.
#to read parquet file:
df = spark.read.format("parquet").load("data.parquet")
df.printSchema()
#output:
  root
 |-- emp_id: integer (nullable = true)
 |-- emp_name: string (nullable = true)
 |-- dept: string (nullable = true)
 |-- salary: double (nullable = true)
#we can see that spark automatically identified metadata of column because parquet stores both data and its metadata. so there is no need to use inferSchema() or predefined column datatypes.

#to read multiple parquet files available inside a folder
df = spark.read.format("parquet").load("/input/*.parquet")

#to read ORC file:
df = spark.read.format("orc").load("data.orc")
df.printSchema()

#BONUS TIP
#recursiveFileLookup:
#when data is stored in multiple different sub-folders and you want to access all the data together into one output then recursiveFileLookup is used.
data/
├── 2023/
│   ├── jan/
│   │   └── file1.parquet
│   ├── feb/
│   │   └── file2.parquet
│
└── 2024/
    ├── jan/
    │   └── file3.parquet
    └── feb/
        └── file4.parquet
	
df = spark.read.option("recursiveFileLookup", "true").format("parquet").load("data/")

#working with JSON:
singleline json:
{"emp_id": 1, "name": "Tushar", "dept": {"id": 10, "name": "IT"}, "skills": ["PySpark", "SQL", "Azure"], "projects": [{"name": "DataLakeMigration", "duration": 6}, {"name": "ADFOptimization", "duration": 3}], "address": {"city": "Mumbai", "zip": 400022}}
{"emp_id": 2, "name": "Ravi", "dept": {"id": 20, "name": "Finance"}, "skills": ["Python", "ETL"], "projects": [{"name": "ReportingSystem", "duration": 4}], "address": {"city": "Delhi", "zip": 110011}}
{"emp_id": 3, "name": "Ankit", "dept": {"id": 30, "name": "HR"}, "skills": ["Excel", "PowerBI"], "projects": [], "address": {"city": "Pune", "zip": 411001}}

#here struct means dictionary
printSchema():
root
 |-- address: struct (nullable = true)
 |    |-- city: string (nullable = true)
 |    |-- zip: long (nullable = true)
 |-- dept: struct (nullable = true)
 |    |-- id: long (nullable = true)
 |    |-- name: string (nullable = true)
 |-- emp_id: long (nullable = true)
 |-- name: string (nullable = true)
 |-- projects: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- name: string (nullable = true)
 |    |    |-- duration: long (nullable = true)
 |-- skills: array (nullable = true)
 |    |-- element: string (containsNull = true)

df.show(truncate=False)
+-------+-------+-------+---------+--------+------+--------------------------------------------+--------+
|emp_id |name   |dept_id|dept_name|city    |zip   |projects                                   |skill   |
+-------+-------+-------+---------+--------+------+--------------------------------------------+--------+
|1      |Tushar |10     |IT       |Mumbai  |400022|[{DataLakeMigration, 6}, {ADFOptimization, 3}]|PySpark|
|1      |Tushar |10     |IT       |Mumbai  |400022|[{DataLakeMigration, 6}, {ADFOptimization, 3}]|SQL    |
|1      |Tushar |10     |IT       |Mumbai  |400022|[{DataLakeMigration, 6}, {ADFOptimization, 3}]|Azure  |
|2      |Ravi   |20     |Finance  |Delhi   |110011|[{ReportingSystem, 4}]                     |Python  |
|2      |Ravi   |20     |Finance  |Delhi   |110011|[{ReportingSystem, 4}]                     |ETL     |
|3      |Ankit  |30     |HR       |Pune    |411001|[]                                         |Excel   |
|3      |Ankit  |30     |HR       |Pune    |411001|[]                                         |PowerBI |
+-------+-------+-------+---------+--------+------+--------------------------------------------+--------+
#reading singleline json
df = spark.read.format("json").load("/data/input/data.json")
df.printSchema()


#multiline json:
[
  {
    "id": 1,
    "name": "Tushar",
    "city": "Mumbai"
  },
  {
    "id": 2,
    "name": "Ankit",
    "city": "Delhi"
  },
  {
    "id": 3,
    "name": "Ravi",
    "city": "Pune"
  }
]

#reading multiline json
[
  {
    "emp_id": 1,
    "name": "Tushar",
    "dept": {
      "id": 10,
      "name": "IT"
    },
    "skills": ["PySpark", "SQL", "Azure"],
    "projects": [
      {
        "name": "DataLakeMigration",
        "duration": 6
      },
      {
        "name": "ADFOptimization",
        "duration": 3
      }
    ],
    "address": {
      "city": "Mumbai",
      "zip": 400022
    }
  },
  {
    "emp_id": 2,
    "name": "Ravi",
    "dept": {
      "id": 20,
      "name": "Finance"
    },
    "skills": ["Python", "ETL"],
    "projects": [
      {
        "name": "ReportingSystem",
        "duration": 4
      }
    ],
    "address": {
      "city": "Delhi",
      "zip": 110011
    }
  }
]

df = spark.read.format("json").option("multiline",True).load("/data/input/data.json")
df.printSchema()
root
 |-- address: struct (nullable = true)
 |    |-- city: string (nullable = true)
 |    |-- zip: long (nullable = true)
 |-- dept: struct (nullable = true)
 |    |-- id: long (nullable = true)
 |    |-- name: string (nullable = true)
 |-- emp_id: long (nullable = true)
 |-- name: string (nullable = true)
 |-- projects: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- name: string (nullable = true)
 |    |    |-- duration: long (nullable = true)
 |-- skills: array (nullable = true)
 |    |-- element: string (containsNull = true)

df.show()
+----------------------+-------------------+-------+-------+----------------------------------------------------+-------------------------+
|address               |dept               |emp_id |name   |projects                                            |skills                   |
+----------------------+-------------------+-------+-------+----------------------------------------------------+-------------------------+
|{Mumbai, 400022}      |{10, IT}           |1      |Tushar |[{DataLakeMigration, 6}, {ADFOptimization, 3}]     |[PySpark, SQL, Azure]    |
|{Delhi, 110011}       |{20, Finance}      |2      |Ravi   |[{ReportingSystem, 4}]                             |[Python, ETL]            |
+----------------------+-------------------+-------+-------+----------------------------------------------------+-------------------------+

#so here spark automatically identifies json metadata and bifercate the data into columns
#but what if we want all of the data in single column and don't want to expand data
#reading the entire JSON record as a single column (a raw string)
{"order_id": "O101", "customer_id": "C001", "order_line_items": [{"item_id": "I001", "qty": 6, "amount": 102.45}, {"item_id": "I002", "qty": 2, "amount": 2.01}], "contact": [9000010000, 9000010001]}

df = spark.read.format("text").load("data/input/data.json")
df.printSchema()
root
 |-- value: string (nullable = true)

#stored all the json data in a single column
df.show(truncate=False)
+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|value                                                                                                                                                                                                |
+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|{"order_id":"O101","customer_id":"C001","order_line_items":[{"item_id":"I001","qty":6,"amount":102.45},{"item_id":"I002","qty":2,"amount":2.01}],"contact":[9000010000,9000010001]}|
+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+

#reading json file with schema:
#means what if we want to read only specific columns out of all the columns of a json file
_schema = "customer_id string, order_id string, contact array<long>"
df = spark.read.format("json").schema(_schema).load("data/input/data.json")
df.show()
+-----------+--------+--------------------+
|customer_id|order_id|             contact|
+-----------+--------+--------------------+
|       C001|    O101|[9000010000, 9000...|
+-----------+--------+--------------------+

#writing json file with schema:
root
 |-- contact: array (nullable = true)
 |    |-- element: long (containsNull = true)
 |-- customer_id: string (nullable = true)
 |-- order_id: string (nullable = true)
 |-- order_line_items: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- amount: double (nullable = true)
 |    |    |-- item_id: string (nullable = true)
 |    |    |-- qty: long (nullable = true)
_schema = "contact array<long>, customer_id string, order_id string, order_line_items array<struct<amount double, item_id string, qty long>>"
#changing above array long to array string as shown below and run it
_schema = "contact array<string>, customer_id string, order_id string, order_line_items array<struct<amount double, item_id string, qty long>>"

df_schema_new = spark.read.format("json").schema(_schema).load("data.json")
df_schema_new.printSchema()
root
 |-- contact: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- customer_id: string (nullable = true)
 |-- order_id: string (nullable = true)
 |-- order_line_items: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- amount: double (nullable = true)
 |    |    |-- item_id: string (nullable = true)
 |    |    |-- qty: long (nullable = true)

df_schema_new.show()
+--------------------+-----------+--------+--------------------+
|             contact|customer_id|order_id|    order_line_items|
+--------------------+-----------+--------+--------------------+
|[9000010000, 9000...|       C001|    O101|[{102.45, I001, 6...|
+--------------------+-----------+--------+--------------------+
#so we can see now contact column as string

#from_json():
#why from_json() is used?
#--> from_json() converts a JSON string column into a structured column (StructType) using a provided schema, allowing Spark to extract and process individual JSON fields.			
but when we read json file, spark automatically detects json data and its metadata and shows the data in different columns and converts it into a structured DataFrame then why from_json() ?
-> when a json format data is stored inside a column then spark consider it as a string so to make it in structured format we use from_json() here.
Eg,
input data:
{"name": "Tushar", "age": 27}
{"name": "Ankit", "age": 30}

df = spark.read.json("data.json")
df.printSchema()
root
 |-- age: long (nullable = true)
 |-- name: string (nullable = true)

#Spark automatically reads JSON format, detects its structure, and splits it into columns (name, age).

BUT, When your JSON is inside a column (as a string).
Here’s where from_json() becomes useful and necessary.
Spark doesn’t automatically parse strings that look like JSON — because they’re just text to Spark.
input data:
| id | json_col                      |
| -- | ----------------------------- |
| 1  | {"name": "Tushar", "age": 27} |
| 2  | {"name": "Ankit", "age": 30}  |

df.printSchema()
root
 |-- id: integer
 |-- json_col: string
#check above output, it has consider json_col datatype as string. so here is the actual usecase of from_json()
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([							#NOTE:we need to define schema first since we are converting json string type to structured type using from_json()
    StructField("name", StringType()),
    StructField("age", IntegerType())
])

df_parsed = df.withColumn("parsed_json", from_json(col("json_col"), schema))
df_parsed.show()
#output:	#here parsed_json is the structured type data which spark can use.
+---+------------------------------+---------------+
|id |json_col                      |parsed_json    |
+---+------------------------------+---------------+
|1  |{"name": "Tushar", "age": 27} |{Tushar, 27}   |
|2  |{"name": "Ankit", "age": 30}  |{Ankit, 30}    |
+---+------------------------------+---------------+

df_parsed.printSchema()
root
 |-- id: long (nullable = true)
 |-- json_col: string (nullable = true)
 |-- parsed_json: struct (nullable = true)
 |    |-- name: string (nullable = true)
 |    |-- age: integer (nullable = true)

#we can now access fields directly.
df_parsed.select("id", "parsed_json.name", "parsed_json.age").show()	
#OR 		
df_parsed.select("id", "parsed_json.*").show()			#to show all the columns use *	
# After from_json() using, you can directly access JSON fields using dot notation.
#output:
+---+-------+---+
|id |name   |age|
+---+-------+---+
|1  |Tushar |27 |
|2  |Ankit  |30 |
+---+-------+---+

#json_col → plain JSON text (string)
#from_json() → converts it to a structured struct column (parsed_json)

#In short, 
from_json() converts a JSON string into real, usable structured data inside Spark.
🔹 It’s needed when your JSON data is stored as text inside a column or file.
🔹 After using it, you can directly access JSON fields using dot notation.

#using nested json:
#input data:
data = [
    (1, '{"emp_id": 101, "details": {"name": "Tushar", "dept": "Finance"}}'),
    (2, '{"emp_id": 102, "details": {"name": "Ankit", "dept": "IT"}}')
]

df = spark.createDataFrame(data, ["id", "json_col"])
df.show(truncate=False)
#output:
+---+--------------------------------------------------------------+
|id |json_col                                                      |
+---+--------------------------------------------------------------+
|1  |{"emp_id": 101, "details": {"name": "Tushar", "dept": "Finance"}}|
|2  |{"emp_id": 102, "details": {"name": "Ankit", "dept": "IT"}}      |
+---+--------------------------------------------------------------+
#So right now, json_col is just a string column — Spark cannot access the nested "name" or "dept" directly.

#define schema:
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("emp_id", IntegerType()),
    StructField("details", StructType([
        StructField("name", StringType()),
        StructField("dept", StringType())
    ]))
])

from pyspark.sql.functions import from_json, col

df_parsed = df.withColumn("parsed_json", from_json(col("json_col"), schema))
df_parsed.show(truncate=False)
#output:
+---+--------------------------------------------------------------+-----------------------------+
|id |json_col                                                      |parsed_json                  |
+---+--------------------------------------------------------------+-----------------------------+
|1  |{"emp_id": 101, "details": {"name": "Tushar", "dept": "Finance"}}|{101, {Tushar, Finance}}    |
|2  |{"emp_id": 102, "details": {"name": "Ankit", "dept": "IT"}}      |{102, {Ankit, IT}}          |
+---+--------------------------------------------------------------+-----------------------------+
df_parsed.printSchema()
root
 |-- id: long (nullable = true)
 |-- json_col: string (nullable = true)
 |-- parsed_json: struct (nullable = true)
 |    |-- emp_id: integer (nullable = true)
 |    |-- details: struct (nullable = true)
 |    |    |-- name: string (nullable = true)
 |    |    |-- dept: string (nullable = true)

#So now, Spark understands the nested structure.
#You can access nested fields using dot notation:
df_parsed.select(
    col("parsed_json.emp_id").alias("emp_id"),
    col("parsed_json.details.name").alias("emp_name"),
    col("parsed_json.details.dept").alias("dept")
).show()
#output:
+------+-------+--------+
|emp_id|emp_name|dept   |
+------+-------+--------+
|101   |Tushar |Finance |
|102   |Ankit  |IT      |
+------+-------+--------+

#example:
#for json array in a string column
#input data:
data = [
    (1, '[{"id": 101, "name": "Tushar", "dept": "Finance"}, {"id": 102, "name": "Ankit", "dept": "IT"}]'),
    (2, '[{"id": 103, "name": "Ravi", "dept": "HR"}]')
]

df = spark.createDataFrame(data, ["batch_id", "json_array"])
df.show(truncate=False)
#output:
+--------+---------------------------------------------------------------------+
|batch_id|json_array                                                           |
+--------+---------------------------------------------------------------------+
|1       |[{"id":101,"name":"Tushar","dept":"Finance"},{"id":102,"name":"Ankit","dept":"IT"}]|
|2       |[{"id":103,"name":"Ravi","dept":"HR"}]                              |
+--------+---------------------------------------------------------------------+

#Right now, json_array is just a string column containing a JSON array.
Since each JSON object inside the array looks like:
{"id": 101, "name": "Tushar", "dept": "Finance"}
We’ll define the schema for one element of the array:
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

employee_schema = ArrayType(
    StructType([
        StructField("id", IntegerType()),
        StructField("name", StringType()),
        StructField("dept", StringType())
    ])
)

from pyspark.sql.functions import from_json, col

df_parsed = df.withColumn("parsed_json", from_json(col("json_array"), employee_schema))
df_parsed.show(truncate=False)

#output:
+--------+---------------------------------------------------------------------+----------------------------------------------+
|batch_id|json_array                                                           |parsed_json                                   |
+--------+---------------------------------------------------------------------+----------------------------------------------+
|1       |[{"id":101,"name":"Tushar","dept":"Finance"},{"id":102,"name":"Ankit","dept":"IT"}]|[{101, Tushar, Finance}, {102, Ankit, IT}]|
|2       |[{"id":103,"name":"Ravi","dept":"HR"}]                              |[{103, Ravi, HR}]                            |
+--------+---------------------------------------------------------------------+----------------------------------------------+

#Now Spark has parsed your JSON string into a real array of structs (ArrayType(StructType(...))).
df_parsed.printSchema()
root
 |-- batch_id: long (nullable = true)
 |-- json_array: string (nullable = true)
 |-- parsed_json: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- id: integer (nullable = true)
 |    |    |-- name: string (nullable = true)
 |    |    |-- dept: string (nullable = true)

#Explode the array to flatten data: If you want to expand (flatten) the array into multiple rows:
from pyspark.sql.functions import explode

df_exploded = df_parsed.select(
    "batch_id",
    explode(col("parsed_json")).alias("emp")
)

df_exploded.show(truncate=False)

+--------+--------------------------+
|batch_id|emp                       |
+--------+--------------------------+
|1       |{101, Tushar, Finance}    |
|1       |{102, Ankit, IT}          |
|2       |{103, Ravi, HR}           |
+--------+--------------------------+

#Access fields inside each object
df_exploded.select(
    "batch_id",
    col("emp.id").alias("emp_id"),
    col("emp.name").alias("emp_name"),
    col("emp.dept").alias("dept")
).show()

#output:
+--------+-------+--------+--------+
|batch_id|emp_id |emp_name|dept    |
+--------+-------+--------+--------+
|1       |101    |Tushar |Finance |
|1       |102    |Ankit  |IT      |
|2       |103    |Ravi   |HR      |
+--------+-------+--------+--------+

#to_json():
#-> to_json() is used to convert structured json data to string json data which is reverse of from_json().
#example:
from pyspark.sql.functions import to_json, struct, col

data = [(101, "Tushar", "Finance"), (102, "Ankit", "IT")]
df = spark.createDataFrame(data, ["emp_id", "name", "dept"])
df.show()

#output:
+------+-------+--------+
|emp_id|name   |dept    |
+------+-------+--------+
|   101|Tushar |Finance |
|   102|Ankit  |IT      |
+------+-------+--------+

#Now, let’s convert multiple columns into a single JSON column.
df_json = df.withColumn("json_col", to_json(struct("emp_id", "name", "dept")))
df_json.show(truncate=False)

#output:
+------+-------+--------+----------------------------------+
|emp_id|name   |dept    |json_col                          |
+------+-------+--------+----------------------------------+
|   101|Tushar |Finance |{"emp_id":101,"name":"Tushar","dept":"Finance"}|
|   102|Ankit  |IT      |{"emp_id":102,"name":"Ankit","dept":"IT"}      |
+------+-------+--------+----------------------------------+

#usecase:
Send structured data as a JSON string (e.g., to Kafka, APIs, or downstream systems).
Store JSON inside a single column instead of multiple columns.
Serialize nested structures easily.

#nested structure example:
df_nested = df.withColumn(
    "json_col",
    to_json(struct(
        col("emp_id"),
        struct(col("name"), col("dept")).alias("details")
    ))
)
df_nested.show(truncate=False)

#output:
+------+-------+--------+---------------------------------------------+
|emp_id|name   |dept    |json_col                                     |
+------+-------+--------+---------------------------------------------+
|101   |Tushar |Finance |{"emp_id":101,"details":{"name":"Tushar","dept":"Finance"}}|
|102   |Ankit  |IT      |{"emp_id":102,"details":{"name":"Ankit","dept":"IT"}}      |
+------+-------+--------+---------------------------------------------+


#explode:
#-> explode() is a Spark SQL function used to convert an array column into multiple rows
#It’s used when you have nested data like arrays or maps inside a DataFrame column and you want to flatten it.
from pyspark.sql.functions import explode
data = [
    (1, ["apple", "banana", "mango"]),
    (2, ["grape", "orange"]),
    (3, [])
]
df = spark.createDataFrame(data, ["id", "fruits"])
df.show(truncate=False)

#output:
+---+----------------------+
|id |fruits                |
+---+----------------------+
|1  |[apple, banana, mango]|
|2  |[grape, orange]       |
|3  |[]                    |
+---+----------------------+

from pyspark.sql.functions import explode

df_exploded = df.select("id", explode("fruits").alias("fruit"))
df_exploded.show()

#output:
+---+------+
| id| fruit|
+---+------+
|  1| apple|
|  1|banana|
|  1| mango|
|  2| grape|
|  2|orange|
+---+------+
#The array is split into multiple rows.
#Row 3 with an empty array is removed (no output rows for empty arrays).

#What if you want to keep empty arrays as nulls?
#Use explode_outer() instead:
from pyspark.sql.functions import explode_outer

df_outer = df.select("id", explode_outer("fruits").alias("fruit"))
df_outer.show()

#output:
+---+------+
| id| fruit|
+---+------+
|  1| apple|
|  1|banana|
|  1| mango|
|  2| grape|
|  2|orange|
|  3|  null|
+---+------+

#posexplode():
#posexplode() is an extention to explode().
#it also returns the position (index) (starting from 0) of each element in the array or map.

from pyspark.sql.functions import posexplode

data = [
    (1, ["apple", "banana", "mango"]),
    (2, ["grape", "orange"])
]
df = spark.createDataFrame(data, ["id", "fruits"])
df.show(truncate=False)

#output:
+---+----------------------+
|id |fruits                |
+---+----------------------+
|1  |[apple, banana, mango]|
|2  |[grape, orange]       |
+---+----------------------+

df_pos = df.select("id", posexplode("fruits").alias("pos", "fruit"))
df_pos.show()

#output:
+---+---+------+
| id|pos| fruit|
+---+---+------+
|  1|  0| apple|
|  1|  1|banana|
|  1|  2| mango|
|  2|  0| grape|
|  2|  1|orange|
+---+---+------+

#What if we want to keep empty arrays?
#Use posexplode_outer() instead.
from pyspark.sql.functions import posexplode_outer

data = [
    (1, ["apple", "banana"]),
    (2, [])
]
df = spark.createDataFrame(data, ["id", "fruits"])
df_outer = df.select("id", posexplode_outer("fruits").alias("pos", "fruit"))
df_outer.show()

#output:
+---+----+------+
| id| pos| fruit|
+---+----+------+
|  1|   0| apple|
|  1|   1|banana|
|  2|null|  null|
+---+----+------+

#to check how many cores we have to run task parallely
spark.sparkContext.defaultParallelism
#output
8
#to check from spark UI:
#-> go to spark UI > Executors tab > we can find number of executors available and its cores. 

#to check number of partitions
df.rdd.getNumPartitions()
#output
8

#to write the data in spark
df.write.format("parquet").save("/output/data/file.parquet")
df.write.format("csv").option("header",True).save("/output/data2/file2.csv")

#since writing is an action, so in spark UI > Jobs tab, we can see a job starts with name "save at ..."
#the number of partitions are equal to the number of parquet/csv files which gets created after running the job. so since there are 8 partitions so it will create 8 partitioned parquet/csv files.
#and the data will get divided within these 8 partitions.

#to check partition information
#each row will show its partition id in which it was used. since we have 8 partitions so the rows are assigned partitions between 0 to 7.
from pyspark.sql.functions import spark_partition_id
df.withColumn("partition_id",spark_partition_id()).show()

#partitionBy():
#partitionBy controls how data is physically written into folders/partitions when you save a DataFrame to storage (like Parquet, ORC, CSV).
#It is used ONLY with write, not with read or transformations.
#Partitioning helps when:
#You have large datasets
#You frequently filter by certain columns
#You want faster reads
#You want organized folders (e.g., by date, country, year)

df.write.format("csv").partitionBy("dept_id").option("header",True).save("data/output/file1")
#here it will create output files based on dept_id column value

#example:
df.write.format("parquet").partitionBy("col1", "col2").save("data/output/file2")
path/
 ├── col1=value1/  
 │     ├── col2=value1/part-xxxxx.snappy.parquet  
 │     └── col2=value2/part-yyyyy.snappy.parquet  
 ├── col1=value2/  
       └── col2=value3/part-zzzzz.snappy.parquet  

#example2:
df.write \
  .partitionBy("category", "year") \
  .parquet("/mnt/data/sales")

/mnt/data/sales/
 ├── category=A/year=2024/part.parquet
 ├── category=A/year=2025/part.parquet
 ├── category=B/year=2024/part.parquet
 └── category=B/year=2025/part.parquet

Benefits of partitionBy
✔️ Faster reading:
When you filter on partitioned columns, Spark reads only those folders, not entire dataset.
df = spark.read.parquet("/mnt/data/sales")
df_filtered = df.filter("category = 'A'")
#Spark will read only folder:
#output:
category=A/

✔️ Good performance for time-series data
Partitioning by date columns is very common:
year
month
day

2️⃣ Avoid over-partitioning

If you partition on too many columns, Spark will create:

thousands of folders

each with tiny files
➡️ BAD for performance

Example of over-partitioning:
df.write.partitionBy("country", "state", "city", "pin_code")

#real scenario
You have 20 records
dept_id values range from 1 to 40
Only 20 dept_ids exist out of 40 possible values
(Some dept_ids between 1–40 DO NOT appear in your data)

df.write.partitionBy("dept_id").parquet("/mnt/data/output")

Spark will create only folders for dept_ids present in the DataFrame
Not all 1 to 40.
Important: Spark does NOT create empty folders
If data contains only these dept_ids:
[1, 3, 5, 8, 10, 11, 15, 20, 30, 33, 40]   (for example)
#output:
output/
 ├── dept_id=1/
 ├── dept_id=3/
 ├── dept_id=5/
 ├── dept_id=8/
 ├── dept_id=10/
 ├── dept_id=11/
 ├── dept_id=15/
 ├── dept_id=20/
 ├── dept_id=30/
 ├── dept_id=33/
 └── dept_id=40/

Even though dept_id range is 1–40, only actual values in the DataFrame create folders.'
Inside each folde, Only rows belonging to that dept_id are saved:
dept_id = 3 folder
output/dept_id=3/
   part-0000.parquet
Contains only rows where dept_id = 3.

so your input data:
| dept_id | count  |
| ------- | ------ |
| 5       | 4 rows |
| 10      | 2 rows |
| 15      | 6 rows |
| 22      | 3 rows |
| 39      | 5 rows |

output:
output/
 ├── dept_id=5/   (4 rows)
 ├── dept_id=10/  (2 rows)
 ├── dept_id=15/  (6 rows)
 ├── dept_id=22/  (3 rows)
 └── dept_id=39/  (5 rows)

No folder for:
dept_id = 1
dept_id = 2
dept_id = 3
…
dept_id = 40
Unless those appear in the DataFrame.

SO, 
#NOTE: partitionBy creates folders only for values that exist in the DataFrame.
#It does NOT create folders for the full range of possible values.


#What is Partition Pruning?
Spark reads only the folders (partitions) that match your filter condition,
instead of scanning the entire dataset.
This makes queries much faster and reduces I/O cost drastically.

#example:
Let’s say your output structure after partitionBy("dept_id") is:
/data/employees/
 ├── dept_id=5/
 ├── dept_id=10/
 ├── dept_id=15/
 ├── dept_id=22/
 └── dept_id=39/

These folders were created because only these dept_ids existed in the DataFrame.
Now you read the data
df = spark.read.parquet("/data/employees")

Case 1: Filter on the Partition Column (GOOD)
result = df.filter("dept_id = 15")
result.show()

Spark will read ONLY this folder:
dept_id=15/

Because Spark knows:
partition column = dept_id
filter = dept_id = 15
So only the folder dept_id=15 needs to be read.
This becomes extremely fast.

Case 2: Filter on multiple partition columns
If you partition like:
df.write.partitionBy("year", "month")

year=2024/month=1/
year=2024/month=2/
year=2025/month=1/

df.filter("year = 2024 AND month = 1")

Reads ONLY:
year=2024/month=1/

If you filter on a non-partition column:
df.filter("salary > 50000")
❌ Spark cannot prune partitions
So it must read ALL folders:
dept_id=5/
dept_id=10/
dept_id=15/
dept_id=22/
dept_id=39/
Then apply the filter after reading.

df.filter("dept_id IN (10, 22)")
✔️ Spark reads ONLY:

dept_id=10/
dept_id=22/

df.filter("upper(dept_id) = '10'")
❌ No pruning if we apply any function on partitioned column
Spark must read all partitions in above case.

df.filter(col("dept_id") == 10)
✔️ Partition will be pruned if col is used.

DataFrame API vs SQL
Both support pruning:
df.where(col("dept_id") == 39)
AND
spark.sql("SELECT * FROM emp WHERE dept_id = 39")
✔️ Both API and SQL prune partitions.

If each partition folder has large files:
Reading all partitions may take minutes
Reading only 1 folder may take seconds
This is why partition columns should match the columns you filter MOST OFTEN.

| Action                               | Partition Pruned?                      |
| ------------------------------------ | -------------------------------------- |
| `dept_id = 10`                       | ✔️ Yes                                 |
| `dept_id IN (10,20)`                 | ✔️ Yes                                 |
| `dept_id BETWEEN 10 AND 20`          | ✔️ Yes                                 |
| `upper(dept_id) = '10'`              | ❌ No                                   |
| `salary > 25000`                     | ❌ No                                   |
| Filter on non-partition column       | ❌ No                                   |
| Filter on multiple partition columns | ✔️ Yes                                 |
| Filter using column expression       | ✔️ Only if Spark can infer exact value |

🔥 Recommended Partitioning Strategy:
| Data Type        | Best Partition Column |
| ---------------- | --------------------- |
| Transaction data | year, month, day      |
| Events / logs    | date, hour            |
| Customer data    | region, country       |
| Finance data     | fiscal_year           |
| Employee data    | dept_id               |
| Time series      | year/month            |
 	

#write mode:
1. append : append new data to existing data
2. overwrite : overwrite new data with existing data
3. ignore : ignore the new data if the same data already exists
4. errorifexists : You want the pipeline to fail if data/output folder already exists, Prevent accidental overwrite/append

df.write.mode("append").option("header",True).parquet("path")
df.write.mode("overwrite").option("header",True).parquet("path")
df.write.mode("ignore").option("header",True).parquet("path")
df.write.mode("errorifexists").option("header",True).parquet("path")

#what if we want to write only 1 partition file to share it with downstream?
df.write.format("csv").repartition(1).mode("overwrite").option("header",True).save("data/file1")

#NOTE: the number of partitions  = the number of output files
#.repartition(5) -> 5 partitions -> 5 output files.

#how spark works under cluster environment?
driver node -> resource manager -> cluster (worker node1 + worker node2)

#IMPORTANT:
HOW SPARK WORK INTERNALLY?:
In Client mode: the driver runs in client machine/laptop.
In CLuster mode: the driver program runs in one of the executors inside the worker node. the resource manager will create one special driver executor for the driver program.
1. when we submit our spark program or run databricks notebook, the driver program starts and creates a sparkSession. Driver asks Cluster Manager for the required resources.
#SparkSession = Entry point → communicates with Cluster Manager.
Driver says:
"I need X executors"
"Each executor needs Y cores & Z memory"
2. eg, you asked for 4 executors with 2 CPU cores each equals to 8 CPU cores.
3. now the resource manager will connect with the cluster and it will create the required executors inside the allocated worker nodes.
4. as per our calculation, for each worker node, it created two executors. and for each executor it will create two CPU cores.
5. once the resource is allocated in each worker node, resource manager will get back to driver program with the required information.
6. once the information is supplied to the driver program, then the driver builds DAG (Directed Acyclic Graph) as Spark never executes transformations immediately.
Every transformation (filter, select, map) builds a logical execution plan called DAG.
Narrow dependencies & wide dependencies are identified.
7. DAG is submitted to DAG Scheduler. Scheduler spilts DAG into stages based on shuffle boundaries.
Before shuffle → Stage 1
After shuffle → Stage 2
After another shuffle → Stage 3
… and so on.
8. For each Stage, Spark creates Tasks. Each input partition = 1 task, Example: 200 partitions → 200 tasks
	starts DIRECTLY connecting with the executors to execute the code.
7. then driver node will send our python program or application code to all the allocated executors.
8. once the python program is available in all the executors then the spark session will instruct the tasks to perform in each core.
9. now all the CPU cores will have their tasks that they need to perform in order to process the data.
10. and once the processing is done, executors will report back to the driver program with the required result.
11. based on the result status whether it succeeded or failed. driver program will now communicate again with resource manager to shutdown the allocated resources.
12. once the resource manager gets all the information, it will scrap off all the executors it has created for that application.

Resource manager/ cluster manager -> responsible for allocating resource to the driver program.
there are four types of cluster manager:
1. standalone: spark cluster
2. YARN: hadoop cluster
3. Mesos: no longer available
4. Kubernetes: for containerize environment

Spark Master UI: http://localhost:8080
Spark Worker UI: http://localhost:8081
Spark App UI:    http://localhost:4040

Inside Spark UI > 2 worker nodes URL with stats like Cores and Memory for each.

after running spark  with default settings:
from pyspark.sql. imporrt SparkSession
spark = (
	SparkSession
	.builder
	.appName("cluster execution")
	.master("local[*]")
	.getOrCreate()
)
go to Executors Tab > we can see three executors: 0 , 1 , driver with each executor in each worker node having each 8 cores with 1028mb each executor memoryand in total 16 tasks.

#lets make it to 4 executors total i.e. two execuotrs in each worker nodes and each executor carry 4 cores with 512mb size of each executor memory:
from pyspark.sql implort SparkSession
spark = (
	SparkSession
	.builder
	.appName("standalone cluster")
	.master("standalone://ID:PORT")
	.config("spark.executor.instances",4)
	.config("spark.executor.cores",4)
	.config("spark.executor.memory","512M")
	.getOrCreate()
)

#now from above code:
go to Executors Tab > we can see four executors: 0 , 1 , 2 , 3 (and one driver executor which is running in local) having 4 cores each with 512mb each executor memory. 
so all the 4 tasks are running in parallel.


but the above spark configuration is not the best way. the best way to configure spark is through spark submit command used to execute code in cluster machine.
bu using spark submit command:
first we create sparkSession:
from pyspark.sql import sparkSession
spark = (
	sparkSession
	.builder						#we are not providing .master() b/c we will provide that in our spark submit command during runtime.
	.appName("cluster execution")
	.getOrCreate()
)
spark

spark submit command:
./bin/spark-submit --master spark://ID:PORT --num-executors 4 --executor-cores 2 --executor-memory 512M /home/file1.py

cluster type:
| Mode                   | Description                                                                                                |
| ---------------------- | ---------------------------------------------------------------------------------------------------------- |
| **local[*]**           | Run Spark **locally using all CPU cores**. No cluster. Good for testing.                                   |
| **Standalone Cluster** | Run Spark on **multiple machines** with distributed workers and executors. Good for large-scale workloads. |

mode type:
a. Client Mode:
Driver runs on your local machine (where you submit the job)
Executors run on cluster nodes
If your laptop dies → job fails

b. Cluster Mode:
Driver runs inside the cluster
Spark master or worker launches it
Your laptop can disconnect, job still runs

#NOTE: local is NOT client mode. both are completely different concepts
putting it all together:
| Cluster Manager | Deploy Mode      | Meaning                               |
| --------------- | ---------------- | ------------------------------------- |
| **local**       | *No deploy mode* | Spark runs fully on your machine      |
| **standalone**  | client           | Driver = laptop, Executors = cluster  |
| **standalone**  | cluster          | Driver = cluster, Executors = cluster |

local means Spark runs locally
standalone means Spark runs on a real cluster
client/cluster mode simply decide where driver runs.

✅ How JVMs Are Used in Spark?
Spark is written in Scala, which runs on the JVM.
So every Spark component (driver, executors) runs inside separate JVM instances.
Executors NEVER share JVMs.

Driver and executors each run in its own separate JVM.
Submit Machine / Client
+------------------------+
|   DRIVER JVM           |
+-----------+------------+
            |
            v
----------------------------------------------
 Worker Node 1          Worker Node 2
+------------------+   +------------------+
| EXECUTOR JVM 1   |   | EXECUTOR JVM 2   |
| Threads (tasks)  |   | Threads (tasks)  |
+------------------+   +------------------+
----------------------------------------------
🧩 How JVM relates to parallelism
Each executor JVM:
Has multiple threads
Each thread runs one Spark task
Threads = cores assigned to executor

how you configure?
--executor-cores 4  
--executor-memory 8G

Meaning:
Executor JVM has 4 task threads
It can run 4 tasks at a time
It has 8 GB heap memory

🛠️ Why Spark uses multiple JVMs (and not one)?
✔ Fault isolation: If one executor JVM crashes, only tasks on that executor fail—not entire program.

What exactly is a CPU core in Spark?
In Spark, a CPU core = ONE parallel task slot.
That means:
1 CPU core = Spark can run 1 task at a time

🧠 Why does Spark need CPU cores?

Spark runs work in parallel.
Parallelism only increases when you have more CPU cores.
1 core → 1 task at a time
4 cores → 4 tasks at a time
16 cores → 16 tasks at a time
More cores = more speed


🖥 Example 1 — Simple parallelism

Imagine your DataFrame has 8 partitions.

Case A: Executor has 1 core
Executor:
 Cores = 1

Tasks:
 [T1] -> [T2] -> [T3] -> ... -> [T8]   (serial)


Runs 8 tasks one by one.
Very slow.


Case B: Executor has 4 cores
Executor:
 Cores = 4

Tasks:
 T1 T2 T3 T4   (runs together)
 T5 T6 T7 T8   (next batch)


Runs 4 tasks at a time.
Twice as fast (for 8 tasks).

Case C: Executor has 8 cores
Executor:
 Cores = 8

Tasks:
 T1 T2 T3 T4 T5 T6 T7 T8   (runs all together)
Runs all tasks in parallel → fastest.

		  
🔥 CPU Cores control Spark’s parallel processing
👉 Example:
--executor-cores 4

This means:
Executor can run 4 tasks simultaneously
Executor JVM will have 4 task threads

how dataframe partitions are different than cluster partitions?
DataFrame partitions = how your data is split
A Spark DataFrame is divided into multiple partitions, and each partition is processed by one task.
100 MB DataFrame → 10 partitions → each ~10 MB
df.rdd.getNumPartitions()

Cluster partitions (resources) = how your cluster is split (nodes, executors, cores, memory)
These refer to how your cluster resources are divided, not your data.

🔢 How to calculate number of partitions by size for our data?
Step 1: Get file size (example: 1 GB = 1024 MB)
Step 2: Divide by ideal partition size

Example:

1024 MB / 128 MB = 8 partitions				#since ideal partition size is 128mb so dividing by it.
			
Step 3: Apply using repartition() if we want to increase or decrease the partition size.
df = df.repartition(8)
Now each partition ≈ 128MB.

we can change partition size while reading the file by changing the config:
Default = 128 MB
		
To reduce partition size to 64MB:
spark.conf.set("spark.sql.files.maxPartitionBytes", 64 * 1024 * 1024)	# 64 MB

OR

To increase partition size to 256MB:
#changed partition size from its default 128mb to 256mb:
spark.conf.set("spark.sql.files.maxPartitionBytes", 256 * 1024 * 1024)  # 256 MB		#assume we applied this so 1gb file will be splitted in 4 parts while reading

#then read the file:
df = spark.read.csv("data.csv")
print(df.rdd.getNumPartitions())
#output:
4				#since we have set the partition size to 256mb.
#1 GB file / 256 MB per partition = 4 partitions
👉 above config only affects the moment when the file is read.

#performing shuffle
df2 = df.groupBy("_c0").count()
print(df2.rdd.getNumPartitions())
#output
200

#Because the default numbre of partitions created:
spark.sql.shuffle.partitions = 200

Even though input had 4 partitions,
after shuffle → 200 partitions.

‼️ This clearly proves shuffle settings override the earlier partition count.

Change the SHUFFLE partition count:
  
run below code before running the shuffle:
spark.conf.set("spark.sql.shuffle.partitions", 50)

Now run your groupBy again:

df3 = df.groupBy("_c0").count()
print(df3.rdd.getNumPartitions())

#output:
50

Now shuffle creates 50 partitions, not 200.

👉 This config only affects partitions AFTER a shuffle.
It does NOT affect file reading.

SAME WAY PARTITIONS ARE CREATED WHEN REPARTITION() , COALESCE() OR DURING WRITING THE FILE.
df2 = df.repartition(50)	#create 50 new partitions

df2 = df.coalesce(5)		#reduces number of partition to 5 WITHOUT shuffle

df.write.parquet("path")	#if df has 10 partitions then spark will create 10 partition output files.
#changing it during write time
df.repartition(50).write.parquet("path")		#This creates 50 output partitions, hence 5 output files. this operation is same as above repartition operation.

when dataframe partitions are created?
DataFrame partitions are created when reading files (input splits), during shuffle operations (joins, groupBy), 
or when the user explicitly calls repartition/coalesce, and sometimes automatically through Adaptive Query Execution.

REFER spark_partitions.txt FILE FOR BETTER UNDERSTANDING.

#CPU CORES VS MEMORY:
🍳 CPU Cores → Number of Chefs

More chefs = more dishes cooked at same time.

🍽️ Memory → Size of Kitchen Table / Workspace

Bigger workspace = chefs can handle more ingredients without dropping things.
WE CAN MENTION IN OUR SPARK SUBMIT COMMAND:
--executor-cores 4
--executor-memory 8G

It means:

Each executor will run 4 tasks simultaneously

Each executor gets 8GB RAM to store intermediate and cached data

Spark runs fast only when both CPU + Memory are balanced.

  





















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


