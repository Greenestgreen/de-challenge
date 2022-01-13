from pyspark.sql.functions import row_number, desc
from pyspark.sql.window import Window


class Transform:

    def top_10_by_console(games):

        # Creating  window for ordering by higher metascore partitioned by console.
        windowSpec = Window.partitionBy("console").orderBy(desc("metascore"),desc("userscore"))

        # Getting top 10 games by console by making partitions by console and ordering by higher metascore.
        # Coalesce has been added to write the result in order and on a single file
        return games.withColumn("row_number", row_number().over(windowSpec)).where("row_number <11").coalesce(1)

    def worst_10_by_console(games):

        # Creating  window for ordering by lower metascore partitioned by console.
        windowSpec = Window.partitionBy("console").orderBy("metascore","userscore")

        # Getting worst 10 games by console by making partitions by console and ordering by higher metascore.
        # Coalesce has been added to write the result in order and on a single file.
        return games.withColumn("row_number", row_number().over(windowSpec)).where("row_number <11").coalesce(1)

    def top_10_all(games):
        # Getting  the best 10 games by metascore
        # Coalesce has been added to write the result in order and on a single file.
        return games.orderBy(desc("metascore"), desc("userscore")).limit(10).coalesce(1)

    def worst_10_all(games):
        # Getting the worst 10 games by metascore
        # Coalesce has been added to write the result in order and on a single file.
        return games.orderBy("metascore","userscore").limit(10).coalesce(1)
