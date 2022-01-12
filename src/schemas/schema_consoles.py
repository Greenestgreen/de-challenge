from pyspark.sql.types import  StructType,StructField,IntegerType,StringType,FloatType

#Schema Related to the consoles csv file
class Schema_consoles:
    
    def get_schema():
        return StructType( [StructField("console", StringType(), True), \
                                          StructField("company", StringType(), True)] ) 