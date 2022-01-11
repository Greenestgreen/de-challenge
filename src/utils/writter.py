import os
import logging
from utils.file_rename import File_rename
class Writter():

   

    def write(path,dataframe,name):
        target = os.path.join(path,name)
        dataframe.write.csv(target,header=True,mode="overwrite")
        File_rename.rename_report_file(target, name)
