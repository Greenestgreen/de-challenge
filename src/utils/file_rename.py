import os

class File_rename:

    def rename_report_file(path,name):
        directory_contents = os.listdir(path)

        for f in directory_contents:
            if ".csv" in f and f[0] != '.':
                os.rename(path + "/" + f, path + "/" + name +  ".csv" )
            else:
                os.remove(os.path.join(path,f))
