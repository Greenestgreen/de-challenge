from pyspark.sql.session import SparkSession
from pyspark.sql.types import IntegerType
import pytest
from src.utils.transform import Transform
from src.schemas.schema_result import Schema_result


@pytest.fixture
def spark():
    return SparkSession.builder \
        .appName("GAMES ETL") \
        .getOrCreate()


@pytest.fixture
def schema_result():
    return Schema_result.get_schema()


@pytest.fixture
def result_df(spark, schema_result):
    return spark.read.csv("./test/resources/result.csv", schema=schema_result, header=True)


@pytest.fixture
def result_df_top10(spark, schema_result):
    return spark.read.csv("./test/resources/result_top10.csv", schema=schema_result, header=True)


def test_top_10_by_console(spark, result_df, schema_result):

    expected_df = spark.createDataFrame(
        [
            (97, "Grand Theft Auto V", "X360", 8.3, "Sep 17, 2013", 1),
            (93, "BioShock 1", "X360", 8.5, "Mar 26, 2013", 2),
            (97, "Grand Theft Auto V", "PS3", 8.3, "Sep 17, 2013", 1),
            (95, "The Last of Us", "PS3", 9.2, "Jun 14, 2013", 2),
            (94, "BioShock Infinite", "PS3", 8.5, "Mar 26, 2013", 3)

        ],
        schema_result.add("row_number", IntegerType(), True)
    )

    assert Transform.top_10_by_console(
        result_df).exceptAll(expected_df).count() == 0


def test_worst_10_by_console(spark, result_df, schema_result):

    expected_df = spark.createDataFrame(
        [
            (97, "Grand Theft Auto V", "X360", 8.3, "Sep 17, 2013", 2),
            (93, "BioShock 1", "X360", 8.5, "Mar 26, 2013", 1),
            (97, "Grand Theft Auto V", "PS3", 8.3, "Sep 17, 2013", 3),
            (95, "The Last of Us", "PS3", 9.2, "Jun 14, 2013", 2),
            (94, "BioShock Infinite", "PS3", 8.5, "Mar 26, 2013", 1)

        ],
        schema_result.add("row_number", IntegerType(), True)
    )

    assert Transform.worst_10_by_console(
        result_df).exceptAll(expected_df).count() == 0


def test_top_10_all(spark, result_df_top10, schema_result):
    expected_df = spark.createDataFrame(
        [
            (99, "GAME 1", "X360", 8.3, "Sep 17, 2013"),
            (98, "GAME 2", "X360", 8.5, "Mar 26, 2013"),
            (96, "GAME 3", "PS3", 8.3, "Sep 17, 2013"),
            (96, "GAME 4", "PS3", 9.2, "Jun 14, 2013"),
            (97, "GAME 0", "PS3", 8.5, "Mar 26, 2013"),
            (95, "GAME 5", "PS3", 8.5, "Mar 26, 2013"),
            (93, "GAME 6", "PS3", 8.5, "Mar 26, 2013"),
            (92, "GAME 7", "PS3", 8.5, "Mar 26, 2013"),
            (91, "GAME 8", "PS3", 8.5, "Mar 26, 2013"),
            (90, "GAME 9", "PS3", 8.5, "Mar 26, 2013")


        ],
        schema_result)

    assert Transform.top_10_all(result_df_top10).exceptAll(
        expected_df).count() == 0


def test_worst_10_all(spark, result_df_top10, schema_result):
    expected_df = spark.createDataFrame(
        [
            (88, "GAME 11", "PS3", 8.5, "Mar 26, 2013"),
            (98, "GAME 2", "X360", 8.5, "Mar 26, 2013"),
            (96, "GAME 3", "PS3", 8.3, "Sep 17, 2013"),
            (96, "GAME 4", "PS3", 9.2, "Jun 14, 2013"),
            (97, "GAME 0", "PS3", 8.5, "Mar 26, 2013"),
            (95, "GAME 5", "PS3", 8.5, "Mar 26, 2013"),
            (93, "GAME 6", "PS3", 8.5, "Mar 26, 2013"),
            (92, "GAME 7", "PS3", 8.5, "Mar 26, 2013"),
            (91, "GAME 8", "PS3", 8.5, "Mar 26, 2013"),
            (90, "GAME 9", "PS3", 8.5, "Mar 26, 2013")


        ], schema_result)

    assert Transform.worst_10_all(
        result_df_top10).exceptAll(expected_df).count() == 0
