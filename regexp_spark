To extract error lines with timestamps from a log file in PySpark, you can follow these steps:.

Load the log file into a DataFrame or RDD.
Use a regular expression to identify lines with error messages and extract the timestamps.
Filter the lines that contain errors.
Optionally, convert the timestamp into a proper format (if necessary).
Here’s a PySpark script that demonstrates this:

Sample Log Format (Assumed):
------------------
2024-10-21 12:30:15 INFO Starting process...
2024-10-21 12:30:20 ERROR Process failed due to connection timeout
2024-10-21 12:31:15 INFO Retrying process...
2024-10-21 12:31:25 ERROR Process failed again due to insufficient memory
------------------
                                                        
PySpark Code:
python
Copy code
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col

# Initialize Spark session
spark = SparkSession.builder.appName("ErrorLogExtraction").getOrCreate()

# Load log file into a DataFrame (assuming the log is in text format)
log_file_path = "path_to_your_log_file.txt"
logs_df = spark.read.text(log_file_path)

# Define a regular expression pattern to extract timestamp and error messages
# Assuming the log format has a timestamp at the beginning of each line, followed by log level and message
timestamp_pattern = r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})"  # Pattern to capture timestamp
error_pattern = r"ERROR"  # Pattern to identify error lines

# Extract the timestamp from each log line
logs_with_timestamp_df = logs_df.withColumn("timestamp", regexp_extract(col("value"), timestamp_pattern, 1))

# Filter the rows that contain 'ERROR' keyword
error_logs_df = logs_with_timestamp_df.filter(logs_with_timestamp_df["value"].contains(error_pattern))

# Select and display the timestamp and error message
error_logs_with_timestamp_df = error_logs_df.select("timestamp", "value")
error_logs_with_timestamp_df.show(truncate=False)

# Optionally, save the result to a file
output_path = "path_to_output_error_log.txt"
error_logs_with_timestamp_df.write.mode("overwrite").text(output_path)
Explanation:
logs_df = spark.read.text(log_file_path): Reads the log file into a DataFrame, with each line treated as a single string in the "value" column.
regexp_extract: Extracts the timestamp using the defined regex pattern.
filter(logs_with_timestamp_df["value"].contains(error_pattern)): Filters out the lines that contain the "ERROR" keyword.
select("timestamp", "value"): Displays the timestamp and the full error line.
Saving to file: The result can be saved to a text file for further analysis.

Output (Sample):
+-------------------+------------------------------------------------------------+
|timestamp          |value                                                       |
+-------------------+------------------------------------------------------------+
|2024-10-21 12:30:20|2024-10-21 12:30:20 ERROR Process failed due to connection timeout|
|2024-10-21 12:31:25|2024-10-21 12:31:25 ERROR Process failed again due to insufficient memory|
+-------------------+------------------------------------------------------------+
This code can be adapted to any log file format by modifying the regular expressions accordingly.
