import os
import base64
import time
from jrdb.client import JRDBClient
from jrdb.repo import JRDBDataGCSRepo
from jrdb import urlcodec
from logging import basicConfig, DEBUG
basicConfig(level=DEBUG)


def main(data, context):
    auth = (os.environ['JRDB_ID'], os.environ['JRDB_PW'])
    client = JRDBClient(auth)
    repo = JRDBDataGCSRepo(os.environ['OUTPUT_BUCKET_NAME'])
    urls = urlcodec.decode(base64.b64decode(data['data']))

    for url in urls:
        jrdb_data_list = client.fetch_jrdbdata(url)
        for jrdb_data in jrdb_data_list:
            repo.store(jrdb_data)
        time.sleep(1)
