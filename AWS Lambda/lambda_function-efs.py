import os
import boto3
import csvprofiler as cp

s3 = boto3.client('s3')
bucket = 'mybucket'     # <-- modify

def lambda_handler(event, context):
    
    # code for moving files from S3 to EFS
    # s3.download_file(bucket, 'config/input.cfg', '/mnt/efs/csvprofiler/config/input.cfg')
    # print(os.system(f'ls /mnt/efs -ahlR'))
    # return
    
    # code to run csvprofiler
    rc = cp.main('/mnt/efs/csvprofiler/config/input.cfg')
    # dirlist = os.listdir('/mnt/efs/csvprofiler/output')
    # for f in dirlist:
    #     s3.upload_file(f'/mnt/efs/csvprofiler/output/{f}', bucket, f'output/{f}')
    print(os.system('ls /mnt/efs -ahlR'))
    return f'rc = {rc}'