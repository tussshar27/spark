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



from pyspark.sql.types import *
from pyspark.sql.functions import *

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

df.createOrReplaceGlobalTempView('table1')
spark.sql("select * from table1")

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


