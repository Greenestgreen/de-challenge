from pyspark.sql import SparkSession
from utils.transform import Transform
from utils.constant import Constant
from utils.reader import Reader
from utils.writter import Writter
from schemas.schema_consoles import Schema_consoles
from schemas.schema_result import Schema_result

if __name__ == "__main__":
    
    spark = SparkSession.builder \
                .master("local[*]") \
                .appName("GAMES ETL") \
                .getOrCreate()
    
    
    #spark.sparkContext.setLogLevel("WARN")
    reader = Reader(spark)
    


    result = reader.read(Constant.INPUT_RESULT, Schema_result.get_schema())
    consoles = reader.read(Constant.INPUT_CONSOLES, Schema_consoles.get_schema())
    
    result_consoles = result.join(consoles, ['console'] )

    Writter.write(Constant.OUTPUT_FOLDER,Transform.top_10_by_console(result_consoles), Constant.TOP_10_CONSOLE_CSV) 
    Writter.write(Constant.OUTPUT_FOLDER,Transform.worst_10_by_console(result_consoles), Constant.WORST_10_CONSOLE_CSV)
    Writter.write(Constant.OUTPUT_FOLDER,Transform.top_10_all(result_consoles), Constant.TOP_10_ALL)
    Writter.write(Constant.OUTPUT_FOLDER,Transform.worst_10_all(result_consoles), Constant.WROST_10_ALL)