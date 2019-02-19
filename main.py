import os
import requests
import zlib
import re
import io
import zipfile
import base64
from jrdbparser import JRDBParser, JRDBAvroWriter, schema
from google.cloud import storage

re_schema_type = re.compile(r'([A-Z]*?)([0-9]*?)\.txt')
parsers = {
    'kab': JRDBParser(schema.kab)
}
writers = {
    'kab': JRDBAvroWriter(schema.kab)
}


def parse(session, url, bucket):
    res = session.get(url, stream=True)
    z = zipfile.ZipFile(io.BytesIO(res.content))

    for filename in z.namelist():
        schema_type, yymmdd = re_schema_type.findall(filename)[0]
        schema_type = schema_type.lower()
        if schema_type not in parsers:
            print('Does not support schema type. [{}]'.format(schema_type))
            continue

        with z.open(filename) as f:
            records = [parsers[schema_type].parse(x)
                       for x in f.read().split(b'\r\n') if x]
            print('Success. [{}]'.format(url))
            buf = io.BytesIO()
            writers[schema_type].write(buf, records)
            blob = bucket.blob(f'{schema_type}/{yymmdd}')
            blob.upload_from_string(buf.getvalue())


def main(data, context):
    session = requests.Session()
    session.mount('http://', requests.adapters.HTTPAdapter(max_retries=3))
    auth = (os.environ['JRDB_ID'], os.environ['JRDB_PW'])
    session.auth = auth

    gcs_cli = storage.Client()
    bucket = gcs_cli.get_bucket(os.environ['OUTPUT_BUCKET_NAME'])

    urls = zlib.decompress(base64.b64decode(data['data'])).decode('utf-8').split(',')
    for url in urls:
        parse(session, url, bucket)
