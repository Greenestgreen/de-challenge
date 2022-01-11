import logging

class Reader():

    def __init__(self,spark):

        self.spark = spark


    def read(self,path,schema):
       #Reading the file and dropping duplicates and rows with null 
       return self.spark.read.csv(path, header=True, schema=schema).dropna(how='any').dropDuplicates()