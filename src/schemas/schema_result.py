from pyspark.sql.types import  StructType,StructField,IntegerType,StringType,FloatType

class Schema_result:

    def get_schema():
        return  StructType( [StructField("metascore", IntegerType(), True), \
                                         StructField("name", StringType(), True), \
                                         StructField("console", StringType(), True), \
                                         StructField("userscore", FloatType(), True), \
                                         StructField("date", StringType(), True)]   )