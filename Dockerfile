FROM jupyter/pyspark-notebook:hadoop-3.2


ENV PYSPARK_MAJOR_PYTHON_VERSION=3

WORKDIR /ETL/
COPY src /ETL/

ENTRYPOINT [ "spark-submit", "main.py"]






