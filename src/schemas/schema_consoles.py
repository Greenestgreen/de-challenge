from pyspark.sql.types import  StructType,StructField,IntegerType,StringType,FloatType

class Schema_consoles:
    
    def get_schema():
        return StructType( [StructField("console", StringType(), True), \
                                          StructField("company", StringType(), True)] ) 