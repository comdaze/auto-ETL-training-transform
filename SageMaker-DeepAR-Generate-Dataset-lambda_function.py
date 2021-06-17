import boto3
import random
import csv
import json
import pandas as pd
import datetime

def write_dicts_to_file(path, data):
    with open(path, 'wb') as fp:
        for d in data:
            fp.write(json.dumps(d).replace('NaN', '"NaN"').encode("utf-8"))
            fp.write("\n".encode('utf-8'))

s3 = boto3.resource('s3')
def copy_to_s3(local_file, s3_path, override=False):
    assert s3_path.startswith('s3://')
    split = s3_path.split('/')
    bucket = split[2]
    path = '/'.join(split[3:])
    buk = s3.Bucket(bucket)
    
    if len(list(buk.objects.filter(Prefix=path))) > 0:
        if not override:
            print('File s3://{}/{} already exists.\nSet override to upload anyway.\n'.format(s3_bucket, s3_path))
            return
        else:
            print('Overwriting existing file')
    with open(local_file, 'rb') as data:
        print('Uploading file to {}'.format(s3_path))
        buk.put_object(Key=path, Body=data)
def lambda_handler(event, context):
    
    bucket_name = 'sagemaker-cn-north-1-xxxxxxx' #改为自己的存储桶
    base_key = 'sagemaker/goldwind/step-function/{}/'.format(datetime.datetime.now().strftime('%Y-%m-%d')) #改为自己的前缀，保留datetime部分
    s3_data_path = "s3://{}/{}".format(bucket_name, base_key)
    
    #df = pd.read_excel('s3://'+bucket_name+'/'+ base_key+'hour_elec.xlsx', index_col='datetime', engine='openpyxl') #改为上传文件的名字，建议固定命名
    
    dfcsv = pd.read_csv('s3://'+bucket_name+'/'+ base_key+'hour_elec.csv')
    df = dfcsv.sort_values(by = 'datetime', ascending = True)
    df = df.set_index('datetime')
    
    freq = '1H'
    prediction_length = 24*7
    training_data = [{'start': str(df.index[0]), 'target': list(df.iloc[:-prediction_length, 0].values)}]
    test_data = [{'start': str(df.index[0]), 'target': list(df.iloc[:, 0].values)}]
    
    # print('training_data:', training_data)
    # print('test_data:', test_data)
  
    write_dicts_to_file('/tmp/train_'+freq+'.json', training_data)
    write_dicts_to_file('/tmp/test_'+freq+'.json', test_data)
    
    copy_to_s3("/tmp/train_"+freq+".json", s3_data_path + "train/train_"+freq+".json", override=True)
    copy_to_s3("/tmp/test_"+freq+".json", s3_data_path + "test/test_"+freq+".json", override=True)
  

    return {
        'statusCode': 200,
        'body':{
        'jobname':'goldwind-deepar-{}'.format(datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S')),
        'modelname':'goldwind-deepar-model-{}'.format(datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S')),
        'output': 's3://'+ bucket_name +'/'+ base_key + 'output/',
        'train': 's3://'+ bucket_name +'/'+ base_key + 'train/train_'+freq+'.json',
        'test': 's3://'+ bucket_name +'/'+ base_key + 'test/test_'+freq+'.json'
        }
    }