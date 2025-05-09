# OpenTableFormat
## Behind the scene of Opentable format

It read the csv/xlsx/json any format data and converted into delta format, but indeed we dont have the delta format file. instead it will convert the any format file into parquet file under opentable format

In delta format it contains 2 files.
1. Parquet file.
2. Transaction files aka (_delta_log) basically a folder concept.

The _delata_log folder contains the json files, schema checkpoint.
1. Basically its keep on tracking the changes you are performing on the file ( basically your transactions).
2. Every time you make the changes, it will keep on adding the new json files. And each time it will not store the state of the table.
3. If you want the latest state of the table, then it will read all the previous json files.

## Note: 
    
If there are 100 transaction, whether we need to read all the 100 json files? No, Every 10th json files its created in delta_log it will maintain the history of the past 9 previous json file in the 10th json file. So you need the current state then it will read the 10th + 1 file (i.e 11th file)

