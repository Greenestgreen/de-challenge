from pyspark.sql import SparkSession
from utils.transform import Transform
from utils.constant import Constant
from utils.reader import Reader
from utils.writter import Writter
from schemas.schema_consoles import Schema_consoles
from utils.clean_result import CleanResult

if __name__ == "__main__":
    
    spark = SparkSession.builder \
                .appName("GAMES ETL") \
                .getOrCreate()
    
    
    spark.sparkContext.setLogLevel("WARN")
    reader = Reader(spark)
    

    #Reading result and consoles csv and setting the schema for them.
    result = reader.read(Constant.INPUT_RESULT)    
    consoles = reader.read(Constant.INPUT_CONSOLES, Schema_consoles.get_schema())   

    #Cleaning userscores on TBD and Casting Dates to YYY-MM-DD format
    clean_result = CleanResult.cast_date(CleanResult.clean_tbd(result))

    #Joining both datasets and just leaving 1 console field in it
    result_consoles = clean_result.join(consoles, ['console'] )

    #Executing the transformation of the data and then writting to the OUTPUT_FOLDER 
    Writter.write(Constant.OUTPUT_FOLDER,Transform.top_10_by_console(result_consoles), Constant.TOP_10_CONSOLE_CSV) 
    Writter.write(Constant.OUTPUT_FOLDER,Transform.worst_10_by_console(result_consoles), Constant.WORST_10_CONSOLE_CSV)
    Writter.write(Constant.OUTPUT_FOLDER,Transform.top_10_all(result_consoles), Constant.TOP_10_ALL)
    Writter.write(Constant.OUTPUT_FOLDER,Transform.worst_10_all(result_consoles), Constant.WROST_10_ALL)

    #Stopping Spark Session
    spark.stop()