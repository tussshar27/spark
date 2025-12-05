https://youtu.be/JB98Loobc7k?si=QBT8bqglWQoqrpO5
https://chatgpt.com/s/t_6932a26ed188819194d75d72adb72c4f

✅ What is a Broadcast Variable in PySpark?
A broadcast variable allows you to send a small dataset (lookup table, config, mapping, list, dictionary, etc.) to all executors only once, instead of sending it with every task.
It helps to reduce network I/O and speed up joins and lookups.

SPARK provides two types of variables in distributed manner which are distributed shared variables:
1. Broadcast Variable
2. Accumulators

from pyspark.sql import SparkSession
spark = (
  SparkSession
  .builder
  .appName("Distributed Shared Variable")
  .master("spark://hostname:port")
  .config("spark.cores.max",16)
  .config("spark.executor.cores",4)
  .config("spark.executor.memory","512M")
  .getOrCreate()
)

#so above code will spinup 4 executors with 4 cores each and 512MB memory.

spark

working of broadcast variable:
1. whenever we create a broadcast variable, it is sent to all the executors that we have.
2. it is cached in all the executors. since we have this partition of data in all the executors so now it can perform join or all the other operations that is required.
3. once the execution is done, all the executors can send out the resultset separately. and they don't need to shuffle data between them.
4. that's why it is known as distributed shared variable because this particular variable is distributed across the cluster.

Example:
dictionary variable:
dept_names = {
1:'dept 1',
2:'dept 2',
3:'dept 3',
4:'dept 4',
5:'dept 5',
6:'dept 6',
7:'dept 7',
8:'dept 8',
9:'dept 9',
10:'dept 10'
}

#to send this variable to all the executors, we need to create a broadcast variable.
#creating a broadcast variable
broadcast_dept_names = spark.sparkContext.broadcast(dept_names)

#to check the type of variable
type(broadcast_dept_names)
#output:
pyspark.broadcast.Broadcast

#to get the value of broadcast variable
broadcast_dept_names.value
#output:
{
1:'dept 1',
2:'dept 2',
3:'dept 3',
4:'dept 4',
5:'dept 5',
6:'dept 6',
7:'dept 7',
8:'dept 8',
9:'dept 9',
10:'dept 10'
}

#now after executing, if you go to jobs then you still won't find any jobs because we haven't hit any action.
#as per theory, we know this variable will be cached in each of the executor.
#we need to create a UDF in order to use it
#create UDF to return department name

from pyspark.sql.functions import udf, col
@udf
def get_dept_names(dept_id):
    return broadcast_dept_names.value.get(dept_id,"unknown")    #NOTE: by passing dept_id key as input, we get dept_name value as output. since it is a dictionary having key and value pair.

#to use above udf, we will create a new column
emp_final = emp.withColumn("dept_name",get_dept_names(col("department_id")))

#even after running above code, there is no job created in spark UI because we haven't called any action.

emp_final.show()

#now go ton spark UI > job > stage: we can see the DAG is happening in a single stage that is because there is no shuffling involved as the broadcast variable pass the data to each executor for operation.
#we have distributed broadcast variable in each of the executor.

#2nd example:
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("BroadcastExample").getOrCreate()

# Small lookup dictionary
country_lookup = {
    1: "India",
    2: "USA",
    3: "UK"
}

# Broadcast it
broadcast_lookup = spark.sparkContext.broadcast(country_lookup)


df = spark.createDataFrame([
    (1, "Tushar"),
    (2, "Rahul"),
    (3, "John")
], ["id", "name"])

# Use broadcast variable inside a UDF
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def lookup_country(id):
    return broadcast_lookup.value.get(id, "Unknown")        #syntax:dict.get(key, default) , so if there is no value for that id then "unknown" will be displayed.

lookup_udf = udf(lookup_country, StringType())

df2 = df.withColumn("country", lookup_udf("id"))
df2.show()


#output
+---+------+---------+
| id| name | country |
+---+------+---------+
|  1|Tushar| India   |
|  2|Rahul | USA     |
|  3|John  | UK      |
+---+------+---------+

#how broadcast variable works internally?
🚀 What Actually Happens Internally When You Use a Broadcast Variable
Assume you write:

lookup = {1: "India", 2: "USA"}
broadcast_lookup = spark.sparkContext.broadcast(lookup)

1️⃣ The broadcast value lives on the DRIVER first
The dictionary {1: "India", 2: "USA"} exists only on the driver when created.
Driver:
    broadcast_lookup.value = {1: "India", 2: "USA"}
No executor knows about it yet.

2️⃣ Spark SERIALIZES the broadcast value
Spark converts the dictionary into a compact binary format.
This reduces size → faster network transfer.

3️⃣ Spark distributes it to ALL executors (only ONCE)
🚫 Spark does NOT send broadcast data with each task
✅ Spark sends it ONCE per executor
Executors receive the serialized broadcast value and store it in their Block Manager memory:
Executor 1:
    broadcast_copy = {1: "India", 2: "USA"}

Executor 2:
    broadcast_copy = {1: "India", 2: "USA"}

Executor 3:
    broadcast_copy = {1: "India", 2: "USA"}
This is why broadcast variables save huge network cost.

4️⃣ Tasks running on executors read the copy locally
Now when you call:
broadcast_lookup.value.get(id)

Each task reads the dictionary from local memory, not from the driver.
⚡ This is extremely fast (no network call).

       Driver
         |
Broadcast only ONCE
         ↓
 ┌───────────────┬───────────────┬───────────────┐
 |   Executor 1   |   Executor 2   |   Executor 3   |
 |  Local copy    |  Local copy    |  Local copy    |
 | {1: 'India'}   | {1: 'India'}   | {1: 'India'}   |
 └───────────────┴───────────────┴───────────────┘
         |
 Tasks on executors read it locally (super fast)

  


#ACCUMULATORS -another type of distributed shared variable.
-> we know that our data is distributed in all executors for a particular department.
-> so our data will rely with different partitions in each of the executor for that particular department. so to calculate sum or count, we have to bring .
Accumulators are variables used for aggregating information across executors in a distributed Spark application.
An Accumulator is a shared variable whose value can only be added to, not read by executors.                                                                                                                                       
#usecase: to calculate total salary of Department 6.















