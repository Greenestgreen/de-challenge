
from pyspark.sql.functions import current_date, date_format, to_date, when
from schemas.schema_result import Schema_result

class CleanResult():

    

    
    def clean_tbd(result):               
        return result.withColumn('userscore', when(result.userscore == 'tbd', '-1').otherwise(result.userscore).cast('float'))

    def cast_date(result):
        return result.withColumn('date', to_date(result.date,'LLL d, yyyy'))

        