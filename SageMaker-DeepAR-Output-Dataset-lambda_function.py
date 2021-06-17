import json
import boto3
import datetime
import pymysql

def lambda_handler(event, context):
    s3 = boto3.resource('s3')
    bucket_name = 'sagemaker-cn-north-1-xxxxxxxx' 
    base_key = 'sagemaker/goldwind/step-function/{}/'.format(datetime.datetime.now().strftime('%Y-%m-%d')) 
    output_file = base_key+'output/test_1H.json.out'
   
    content_object = s3.Object(bucket_name, output_file)
    file_content = content_object.get()['Body'].read().decode('utf-8')
    json_content = json.loads(file_content)
    result=json_content['quantiles']['0.5']
    
    # connect to MySQL
    
    secret_name = "hour_elec_secret"
    region_name = "cn-north-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
    service_name='secretsmanager',
    region_name=region_name
    )
    secret = client.get_secret_value(
             SecretId=secret_name
    )
    secret_dict = json.loads(secret['SecretString'])

    username = secret_dict['username']
    password = secret_dict['password']
    host = secret_dict['host']
    con = pymysql.connect(host = host, user = username, passwd = password, db = 'forcast_result')
    cursor = con.cursor()
    for i in result:
        cursor.execute("INSERT INTO forcast_result_table (forcast_result) VALUES (%s)", (i))
    con.commit()
    con.close()
    return {
        'statusCode': 200,
        'body': json_content['quantiles']['0.5']
    }

