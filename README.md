# Pandas Scripts

2 Python (Pandas) scripts:

read_from_s3_save_to_postgresql - Read market-reference-data.csv  from S3 and dump into a Postgresql table

refdata_joined_with_tickdata - Read Tick data from 2 separate S3 files, clean/transform data and concat to create a single tick-Dataframe. Read ref-data from Postgresql into ref-Dataframe. Join tick-Dataframe with ref-Dataframe

## Note
Please make sure to update keys/secrets/passwords as appropriate.

## License
[MIT](https://choosealicense.com/licenses/mit/)
