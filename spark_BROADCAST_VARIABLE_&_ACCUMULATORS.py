https://youtu.be/JB98Loobc7k?si=QBT8bqglWQoqrpO5

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
    return broadcast_dept_names.value.get(dept_id)    #NOTE: by passing dept_id key as input, we get dept_name value as output. since it is a dictionary having key and value pair.

#to use above udf, we will create a new column
emp_final = emp.withColumn("dept_name",get_dept_names(col("department_id")))

#even after running above code, there is no job created in spark UI because we haven't called any action.

emp_final.show()

#now go ton spark UI > job > stage: we can see the DAG is happening in a single stage that is because there is no shuffling involved as the broadcast variable pass the data to each executor for operation.
#we have distributed broadcast variable in each of the executor.


#ACCUMULATORS -another type of distributed shared variable.
-> we know that our data is distributed in all executors for a particular department.
-> so our data will rely with different partitions in each of the executor for that particular department. so to calculate sum or count, we have to bring 
#usecase: to calculate total salary of Department 6.















