import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, regexp_replace
import mysql.connector
import os


desktop_path = os.path.join(os.path.expanduser("~"), "Desktop")
csv_file_path = os.path.join(desktop_path, "frequent_bigrams.csv")
df_pandas = pd.read_csv(csv_file_path)

spark = SparkSession.builder \
    .appName("CSV to Spark") \
    .getOrCreate()


schema = StructType([
    StructField("term", StringType(), True),
    StructField("counts", IntegerType(), True)
])

df_spark = spark.createDataFrame(df_pandas, schema)


df_spark = df_spark.fillna("")


numeric_columns = ["counts"]

for column in numeric_columns:
    df_spark = df_spark.withColumn(column, regexp_replace(col(column).cast(StringType()), "[^0-9.-]", "").cast(IntegerType()))

df_spark.show()


connection = mysql.connector.connect(
    host="localhost",
    user="root", 
    password="root", 
    database="csv_data_db"  
)

cursor = connection.cursor()


cursor.execute("""
CREATE TABLE IF NOT EXISTS frequent_bigrams (
    term VARCHAR(255),
    counts INT
)
""")


for row in df_spark.collect():
    cursor.execute("""
    INSERT INTO frequent_bigrams (term, counts)
    VALUES (%s, %s)""",
                   (row["term"], row["counts"]))

connection.commit()
cursor.close()
connection.close()