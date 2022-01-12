import pytest
from src.utils.clean_result import CleanResult
from src.schemas.schema_result import Schema_result
from pyspark.sql.session import SparkSession



@pytest.fixture
def spark():
    return SparkSession.builder \
                .appName("GAMES ETL") \
                .getOrCreate()

@pytest.fixture
def schema_result():
    return Schema_result.get_schema()

@pytest.fixture
def result_df(spark,schema_result):
    return spark.createDataFrame(
    [   
        (97,"Grand Theft Auto V","X360","TBD","Sep 17, 2013",2),
        (93,"BioShock 1","X360",8.5,"Mar 26, 2013",1)

    ],
    schema_result)


def test_clean_tbd(result_df,schema_result):
    expected_df =  spark.createDataFrame(
    [   
        (97,"Grand Theft Auto V","X360",-1,"Sep 17, 2013",2),
        (93,"BioShock 1","X360",8.5,"Mar 26, 2013",1)

    ],
    schema_result)

    assert CleanResult.clean_tbd(result_df).exceptAll(expected_df).count() == 0

    