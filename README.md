# OpenTableFormat

Behind the scene of Opentable format

    It read the csv data and converted into delta format, but indeed we dont have the delta format file. instead it will convert the csv file into parquet file

    In delta format it contains 2 files
        Parquet file
        Transaction files aka (_delata_log) basically a folder concept

    The _delata_log folder contains the json files, schema checkpoint
        Basically its keep on tracking the changes you are performing on the file ( basically your transactions)
        every time you make the changes, it will keep on adding the new json files. And each time it will not store the state of the table
        if you want the latest state of the table, then it will read all the previous json files.

    Note: If there are 100 transaction,whether we need to read all the 100 json files? No, Every 10th json files its created in delta_log it will maintain the history of the past 9 previous json file in the 10th json file. So you need the current state then it will read the 10th + 1 file (i.e 11th file)

