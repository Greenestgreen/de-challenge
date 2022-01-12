from pyspark.sql.session import SparkSession
from pyspark.sql.types import IntegerType
import pytest
from src.utils.transform import Transform
from src.schemas.schema_result import Schema_result
from src.utils.clean_result import CleanResult


@pytest.fixture
def spark():
    return SparkSession.builder \
                .appName("GAMES ETL") \
                .getOrCreate()

@pytest.fixture
def schema_result():
    return Schema_result.get_schema().add("row_number", IntegerType(), True)

@pytest.fixture
def result_df(spark,schema_result):
    return spark.createDataFrame(
    [   
        (97,"Grand Theft Auto V","X360","tbd","Sep 17, 2013",2),
        (93,"BioShock 1","X360",8.5,"Mar 26, 2013",1)

    ],
    schema_result)


def test_clean_tbd(spark,result_df,schema_result):
    expected_df =  spark.createDataFrame(
    [   
        (97,"Grand Theft Auto V","X360",-1.0,"Sep 17, 2013",2),
        (93,"BioShock 1","X360",8.5,"Mar 26, 2013",1)

    ],
    schema_result)

    assert CleanResult.clean_tbd(result_df).exceptAll(expected_df).count() == 0

def test_cast_date(spark,result_df,schema_result):
    expected_df =  spark.createDataFrame(
    [   
        (97,"Grand Theft Auto V","X360","tbd","2013-09-17",2),
        (93,"BioShock 1","X360",8.5,"2013-03-26",1)

    ],
    schema_result)
    
    assert CleanResult.cast_date(result_df).exceptAll(expected_df).count() == 0
