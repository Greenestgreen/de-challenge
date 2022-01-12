class Reader():

    #Class constructor to get the sparksesion object
    def __init__(self,spark):

        self.spark = spark


    def read(self,path,schema=None):
       #Reading the file and dropping duplicates and rows with null 

       return self.spark.read.csv(path,schema=schema).dropna(how='any').dropDuplicates()  if schema \
           else self.spark.read.csv(path,header=True).dropna(how='any').dropDuplicates() 

       