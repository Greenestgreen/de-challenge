from pyspark.sql.functions import to_date, when


class CleanResult():

    # This cleans the userscore field changing 'tbd' value to -1 and then casting it to float
    def clean_tbd(result):
        return result.withColumn('userscore', when(result.userscore == 'tbd', '-1').otherwise(result.userscore).cast('float'))

    # This exchange the date display to YYYY-MM-DD and casting it to date
    def cast_date(result):
        return result.withColumn('date', to_date(result.date, 'LLL d, yyyy'))
