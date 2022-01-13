import os


class File_rename:

    # This function rewrites the csv name file written (Ie: part0000-1545h4-3f1f.csv) to the given name.
    def rename_report_file(path, name):
        directory_contents = os.listdir(path)

        for f in directory_contents:
            if ".csv" in f and f[0] != '.':
                os.rename(path + "/" + f, path + "/" + name + ".csv")
            else:
                os.remove(os.path.join(path, f))
