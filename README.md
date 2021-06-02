# auto-ETL-training-transform
# 自动化ETL，训练，推理

## 架构图 

[Image: image.png]
## 1.Glue的设置

### 添加连接 

添加JDBC MySQL数据库连接：进入Glue服务，选择连接-添加连接
[Image: image.png]进入连接向导，设置连接属性：连接名称，连接类型选择JDBC
[Image: image.png]设置JDBC数据库连接访问属性
[Image: image.png]预览设置，点击完成

[Image: image.png]


### 添加爬网程序

[Image: image.png][Image: image.png]
[Image: image.png]添加数据存储，选择JDBC，和上一步创建好的连接
[Image: image.png]
[Image: image.png]


[Image: image.png]
[Image: image.png]
[Image: image.png]


[Image: image.png]设置完成后运行爬网程序：
[Image: image.png]
### 查看数据库和表

正常执行完爬网程序后，在表，可以看到爬的表
[Image: image.png]点击这个表，编辑架构，修改数据类型
[Image: image.png]编辑架构：
[Image: image.png]


### 添加作业

[Image: image.png][Image: image.png]

[Image: image.png]
[Image: image.png]选择一个数据目标：在数据目标中创建表，数据存储选择Amazon S3，格式CSV，设定目标路径为一个S3的路径
[Image: image.png]数据架构定义，根据情况修改
[Image: image.png]添加触发器：
[Image: image.png]
设置触发器属性，按计划，注意时间是UTC时间，本地时间为UTC+8小时
[Image: image.png]选择要触发的作业：
[Image: image.png]
## 2.设置Step Functions

创建状态机：
[Image: image.png]定义状态机：选择使用代码段创作，标准类型
[Image: image.png]按照如下json代码，进行定义：

```
{
  "StartAt": "Generate dataset",
  "States": {
    "Generate dataset": {
      "Resource": "arn:aws-cn:lambda:cn-north-1:456370280007:function:SageMaker-DeepAR-Generate-Dataset",
      "Type": "Task",
      "Next": "Train Step",
      "Catch": [
        {
          "ErrorEquals": [
            "States.TaskFailed"
          ],
          "Next": "ML Workflow failed"
        }
      ]
    },
    "Train Step": {
      "Resource": "arn:aws-cn:states:::sagemaker:createTrainingJob.sync",
      "Parameters": {
        "AlgorithmSpecification": {
          "TrainingImage": "390948362332.dkr.ecr.cn-north-1.amazonaws.com.cn/forecasting-deepar:1",
          "TrainingInputMode": "File"
        },
        "OutputDataConfig": {
          "S3OutputPath.$": "$.body.output"
        },
        "StoppingCondition": {
          "MaxRuntimeInSeconds": 86400
        },
        "ResourceConfig": {
          "InstanceCount": 1,
          "InstanceType": "ml.c5.2xlarge",
          "VolumeSizeInGB": 30
        },
        "RoleArn": "arn:aws-cn:iam::456370280007:role/service-role/AmazonSageMaker-ExecutionRole-20200604T105569",
        "InputDataConfig": [
          {
            "DataSource": {
              "S3DataSource": {
                "S3DataType": "S3Prefix",
                "S3Uri.$": "$.body.train",
                "S3DataDistributionType": "FullyReplicated"
              }
            },
            "ChannelName": "train"
          },
          {
            "DataSource": {
              "S3DataSource": {
                "S3DataType": "S3Prefix",
                "S3Uri.$": "$.body.test",
                "S3DataDistributionType": "FullyReplicated"
              }
            },
            "ChannelName": "test"
          }
        ],
        "HyperParameters": {
          "time_freq": "1H",
          "epochs": "4",
          "early_stopping_patience": "40",
          "mini_batch_size": "256",
          "learning_rate": "1E-3",
          "context_length": "672",
          "prediction_length": "168"
        },
        "TrainingJobName.$": "$.body.jobname"
      },
      "Type": "Task",
      "ResultPath": "$.taskresult",
      "Next": "Save model",
      "Catch": [
        {
          "ErrorEquals": [
            "States.TaskFailed"
          ],
          "Next": "ML Workflow failed"
        }
      ]
    },
    "Save model": {
      "Parameters": {
        "ExecutionRoleArn": "arn:aws-cn:iam::456370280007:role/service-role/AmazonSageMaker-ExecutionRole-20200604T105569",
        "ModelName.$": "$.body.modelname",
        "PrimaryContainer": {
          "Environment": {},
          "Image": "390948362332.dkr.ecr.cn-north-1.amazonaws.com.cn/forecasting-deepar:1",
          "ModelDataUrl.$": "$['taskresult']['ModelArtifacts']['S3ModelArtifacts']"
        }
      },
      "Resource": "arn:aws-cn:states:::sagemaker:createModel",
      "Type": "Task",
      "ResultPath": "$.taskresult",
      "Next": "Transform Input Dataset",
      "Catch": [
        {
          "ErrorEquals": [
            "States.TaskFailed"
          ],
          "Next": "ML Workflow failed"
        }
      ]
    },
    "Transform Input Dataset": {
      "Resource": "arn:aws-cn:states:::sagemaker:createTransformJob.sync",
      "Parameters": {
        "TransformJobName.$": "$.body.jobname",
        "ModelName.$": "$.body.modelname",
        "TransformInput": {
          "DataSource": {
            "S3DataSource": {
              "S3DataType": "S3Prefix",
              "S3Uri.$": "$.body.test"
            }
          },
          "SplitType": "Line"
        },
        "TransformOutput": {
          "S3OutputPath.$": "$.body.output",
          "AssembleWith": "Line"
        },
        "TransformResources": {
          "InstanceCount": 1,
          "InstanceType": "ml.m5.xlarge"
        },
        "BatchStrategy": "SingleRecord"
      },
      "Type": "Task",
      "Next": "Output dataset",
      "Catch": [
        {
          "ErrorEquals": [
            "States.TaskFailed"
          ],
          "Next": "ML Workflow failed"
        }
      ]
    },
    "Output dataset": {
      "Resource": "arn:aws-cn:lambda:cn-north-1:456370280007:function:SageMaker-DeepAR-Output-Dataset",
      "Type": "Task",
      "End": true,
      "Catch": [
        {
          "ErrorEquals": [
            "States.TaskFailed"
          ],
          "Next": "ML Workflow failed"
        }
      ]
    },
    "ML Workflow failed": {
      "Cause": "SageMakerProcessingJobFailed",
      "Type": "Fail"
    }
  }
}
```

自动生成流程图：
[Image: image.png]
指定详细信息：
[Image: image.png]按照如下策略创建角色：

```
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "batch:DescribeJobs",
                "batch:SubmitJob",
                "batch:TerminateJob",
                "dynamodb:DeleteItem",
                "dynamodb:GetItem",
                "dynamodb:PutItem",
                "dynamodb:UpdateItem",
                "ecs:DescribeTasks",
                "ecs:RunTask",
                "ecs:StopTask",
                "glue:BatchStopJobRun",
                "glue:GetJobRun",
                "glue:GetJobRuns",
                "glue:StartJobRun",
                "lambda:InvokeFunction",
                "sagemaker:CreateEndpoint",
                "sagemaker:CreateEndpointConfig",
                "sagemaker:CreateHyperParameterTuningJob",
                "sagemaker:CreateModel",
                "sagemaker:CreateProcessingJob",
                "sagemaker:CreateTrainingJob",
                "sagemaker:CreateTransformJob",
                "sagemaker:DeleteEndpoint",
                "sagemaker:DeleteEndpointConfig",
                "sagemaker:DescribeHyperParameterTuningJob",
                "sagemaker:DescribeProcessingJob",
                "sagemaker:DescribeTrainingJob",
                "sagemaker:DescribeTransformJob",
                "sagemaker:ListProcessingJobs",
                "sagemaker:ListTags",
                "sagemaker:StopHyperParameterTuningJob",
                "sagemaker:StopProcessingJob",
                "sagemaker:StopTrainingJob",
                "sagemaker:StopTransformJob",
                "sagemaker:UpdateEndpoint",
                "sns:Publish",
                "sqs:SendMessage"
            ],
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "iam:PassRole"
            ],
            "Resource": "*",
            "Condition": {
                "StringEquals": {
                    "iam:PassedToService": "sagemaker.amazonaws.com"
                }
            }
        },
        {
            "Effect": "Allow",
            "Action": [
                "events:*"
            ],
            "Resource": [
                "arn:aws-cn:events:*:*:rule/*"
            ]
        }
    ]
}
```

## 3.设置EventBridge规则

[Image: image.png][Image: image.png]
定义模式：选择事件模式，事件匹配模式选择自定义模式，并且编辑事件模式：

```
{
"detail-type": [
"Glue Job State Change"
],
"source": [
"aws.glue"
],
"detail": {
"jobName": [
"hour_elec_job"
],
"state": [
"SUCCEEDED"
]
}
}
```

选择目标：Step Function状态机，然后点击创建
[Image: image.png]

## 4.设置Amazon Secrets Manager，存储MySQL的密匙信息：

[Image: image.png][Image: image.png]

[Image: image.png]
[Image: image.png]
[Image: image.png]
## 5.创建Lambda函数

生成Lambda的层的软件依赖包

```
vi requirements.txt
pandas
openpyxl
fsspec
s3fs
sagemaker
pymysql

              
pip3 install -r requirements.txt --target python -i https://opentuna.cn/pypi/web/simple/
zip -q -r Pandas.zip python
aws s3 cp Pandas.zip s3://sagemaker-cn-north-1-456370280007/sagemaker/goldwind/
```

创建层
[Image: image.png]配置层，填写从S3上传的路径，和运行时：
[Image: image.png]
创建两个Lambda函数：SageMaker-DeepAR-Generate-Dataset和SageMaker-DeepAR-Output-Dataset
[Image: image.png]
设置Lambda函数：
[Image: image.png]粘贴如下代码到SageMaker-DeepAR-Generate-Dataset 函数

```
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
    
    bucket_name = 'sagemaker-cn-north-1-456370280007' #改为自己的存储桶
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
```

粘贴如下代码到SageMaker-DeepAR-Output-Dataset函数

```
import json
import boto3
import datetime
import pymysql

def lambda_handler(event, context):
    s3 = boto3.resource('s3')
    bucket_name = 'sagemaker-cn-north-1-456370280007' 
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


```

设置内存和超时：
[Image: image.png]权限 ：Lambda执行角色需要：添加EC2 NetworkInterface相关权限，SecretsManager读权限和基本Lamda基本执行权限
[Image: image.png]权限设定参考
[Image: image.png]VPC设置：
[Image: image.png]
