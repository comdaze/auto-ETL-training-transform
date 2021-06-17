pip install -r requirements.txt --target python -i https://opentuna.cn/pypi/web/simple/
zip -q -r Pandas.zip python
aws s3 cp Pandas.zip s3://app-goldwind/stepfunctions/
