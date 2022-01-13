from pyspark.sql.types import DataType, DateType, StructType, StructField, IntegerType, StringType, FloatType

# Schema related to the result csv file


class Schema_report:

    def get_schema():
        return StructType([StructField("metascore", IntegerType(), True),
                           StructField("name", StringType(), True),
                           StructField("console", StringType(), True),
                           StructField("userscore", FloatType(), True),
                           StructField("date", DateType(), True),
                           StructField("company", StringType(), True)])
