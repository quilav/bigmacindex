import nasdaqdatalink
import pandas as pd
from pandas import DataFrame

from secrets import access_key, secret_access_key, nasdaq_api_key

import boto3
import os

# stworzenie slownika metadata zawierajacego info o krajach, dla ktorych mamy big mac index
metadataDF = pd.read_csv(filepath_or_buffer = 'resources/ECONOMIST_metadata.csv')
codeList = list(metadataDF['code'])

# setup nasdaq data link
nasdaqdatalink.ApiConfig.api_key = nasdaq_api_key

# stare podejscie - eksport kazdego kraju/code ze slownika jako osobny DF/CSV
# for code in codeList:
#     df = nasdaqdatalink.get('ECONOMIST/' + code)
#     df.to_csv(code + '.csv')

mdf = pd.DataFrame ()

for code in codeList:
    df = nasdaqdatalink.get('ECONOMIST/' + code)
    df['code'] = code
    mdf = pd.concat([mdf, df], axis = 0)

mdf.to_csv('big-mac-index-codes.csv')

# setup clienta na AWS S3
client = boto3.client('s3',
                      aws_access_key_id = access_key,
                      aws_secret_access_key = secret_access_key)

# wysylka danych na AWS S3
for file in os.listdir():
    if '.csv' in file:
        upload_file_bucket = 'big-mac-index-bucket'
        upload_file_key = str(file)
        client.upload_file(file, upload_file_bucket, upload_file_key)