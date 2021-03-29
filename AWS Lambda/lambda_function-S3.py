import os
import boto3
import csvprofiler as cp
from configparser import ConfigParser
import shutil

s3 = boto3.client('s3')

# clean up previous / make new temp area
temp = '/tmp/csvprofiler/'
try:
    shutil.rmtree(temp)
    print(f'previous {temp} removed')
except:
    pass
os.mkdir(temp)

def lambda_handler(event, context):
    # get config file, store in temp area
    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    key = event["Records"][0]["s3"]["object"]["key"]
    _, _file = os.path.split(key)
    config = temp + _file
    s3.download_file(bucket, key, config)

    # read in config
    parser = ConfigParser(allow_no_value=True)
    parser.read(config)

    # get input s3 paths, download files to temp area
    # get output s3 paths, save for upload later
    # change input/output paths to temp area for csvprofiler, save new config
    old_parser_values = {}
    for item in parser.items('Paths'):
        if (item[0] in ["csv_file", "param_file"] or item[0].startswith('lookup_') \
            or item[0].startswith('regex_') or item[0].startswith('xcheck_') ) \
            and not item[1].startswith('import '):
            _, _file = os.path.split(item[1])
            downfile = temp + _file
            s3.download_file(bucket, item[1], downfile)
            parser['Paths'][item[0]] = downfile
        elif item[0] in ["report_file", "error_csv_file", "error_log_file"]:
            _, _file = os.path.split(item[1])
            downfile = temp + _file
            old_parser_values[downfile] = item[1]
            parser['Paths'][item[0]] = downfile
    with open(config, 'w') as cf:
        parser.write(cf)
    os.system('cat /tmp/csvprofiler/fl_csvp_s3.cfg')
    print(old_parser_values)

    # call csvprofiler
    rc = cp.main(config)

    print(os.system('ls /tmp/csvprofiler'))
    # if outputs exist, copy to saved s3 locations
    for upfile, s3key in old_parser_values.items():
        if os.path.exists(upfile):
            s3.upload_file(upfile, bucket, s3key)
    shutil.rmtree(temp)
    print(f'{temp} removed')
    print(f'rc = {rc}')
    return f'rc = {rc}'