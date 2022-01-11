from pyspark.sql.functions import *
from pyspark.sql.window import Window
import logging

class Transform:

    def top_10_by_console(games):
        #Getting top 10 games by console by making partitions by console and ordering by higher metascore 
        windowSpec = Window.partitionBy("console").orderBy(desc("metascore"))
        #games.withColumn("row_number", row_number().over(windowSpec)).where("row_number <11").show()
        return games.withColumn("row_number", row_number().over(windowSpec)).where("row_number <11").coalesce(1)

    def worst_10_by_console(games):
        #Getting top 10 games by console by making partitions by console and ordering by lower metascore 
        windowSpec = Window.partitionBy("console").orderBy("metascore")
        #games.withColumn("row_number", row_number().over(windowSpec)).where("row_number <11").show()
        return games.withColumn("row_number", row_number().over(windowSpec)).where("row_number <11").coalesce(1)
    
    def top_10_all(games):
        #Getting  the best 10 games by metascore
        return games.orderBy(desc("metascore")).limit(10).coalesce(1)
    
    def worst_10_all(games):
        #Getting the worst 10 games by metascore
        return games.orderBy("metascore").limit(10).coalesce(1)