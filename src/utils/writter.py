import os
from utils.file_rename import File_rename


class Writter():

    # This functions overwrites any folder by the given name
    # Then changes the name of the file in given folder
    def write(path, dataframe, name):
        target = os.path.join(path, name)
        dataframe.write.csv(target, header=True, mode="overwrite")
        File_rename.rename_report_file(target, name)
