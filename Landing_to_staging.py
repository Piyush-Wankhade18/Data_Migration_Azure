table_name = []

for i in dbutils.fs.ls('mnt/bronze/SalesLT/'):
    table_name.append(i.name.split('/')[0])

#This script fetches the list of all folders (tables) inside the bronze SalesLT layer in Databricks and stores them into a #Python list called table_name.
#i.name is the folder/file name (example: "Address/").
#.split('/') splits it into parts by /.
#[0] takes the first part (to remove the trailing slash).

from pyspark.sql.functions import from_utc_timestamp, date_format
from pyspark.sql.types import TimestampType

#Iterates through each table name from your earlier table_name list,Reads the corresponding Parquet file from Bronze layer.

for i in table_name:
    path = '/mnt/bronze/SalesLT/' + i + '/' + i + '.parquet'
    df = spark.read.format('parquet').load(path)
    column = df.columns

#For each column in the table,If the column name contains "Date" or "date", it converts it to a proper date format (yyyy-MM-dd) in UTC.

    for col in column:
        if "Date" in col or "date" in col:
            df = df.withColumn(col, date_format(from_utc_timestamp(df[col].cast(TimestampType()), "UTC"), "yyyy-MM-dd"))
#Saves the cleaned/transformed data into the Silver layer in Delta format
#overwrite ensures existing data is replaced with the new processed version.
    output_path = '/mnt/silver/SalesLT/' + i + '/'
    df.write.format('delta').mode("overwrite").save(output_path)