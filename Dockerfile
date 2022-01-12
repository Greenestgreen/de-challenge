FROM jupyter/pyspark-notebook:latest

ENV INPUT_RESULT='/ETL/data/result.csv'
ENV INPUT_CONSOLES='/ETL/data/consoles.csv'
ENV OUTPUT_FOLDER='/ETL/data/output'
ENV PYSPARK_MAJOR_PYTHON_VERSION=3



WORKDIR /ETL/
COPY src /ETL/



ENTRYPOINT [ "spark-submit", "main.py", "--deploy-mode", "client"]






