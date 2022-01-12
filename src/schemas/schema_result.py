from pyspark.sql.types import  StructType,StructField,IntegerType,StringType

#Schema related to the result csv file
class Schema_result:

    def get_schema():
        return  StructType( [StructField("metascore", IntegerType(), True), \
                                         StructField("name", StringType(), True), \
                                         StructField("console", StringType(), True), \
                                         StructField("userscore", StringType(), True), \
                                         StructField("date", StringType(), True)]   )