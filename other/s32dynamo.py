import json
import boto3
import datetime
import time

s3_cient = boto3.client('s3')
dynamo_db = boto3.resource('dynamodb')
table = dynamo_db.Table('yelp-restaurants')  # DynamoDB table name


def lambda_handler(event, context):
    # TODO implement
    bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
    s3_file_name = event["Records"][0]["s3"]["object"]["key"]
    resp = s3_cient.get_object(Bucket=bucket_name, Key=s3_file_name)

    data = resp['Body'].read().decode('utf-8')
    timestamp = int(datetime.datetime.utcnow().timestamp())
    print(
        data)  # This print statement will be displayed on Lambda console as well as on CloudWatch. You can also remove this if not required
    restaurant = data.split("\n")
    for res in restaurant:
        res = res.split(",")
        # Exception handling done for skipping errors. When we are dealing with CSV file content, there may be a chance to get last row as empty row. So I have used try-catch for avoiding the runtime issues.
        try:

            table.put_item(
                Item={
                    "Id": str(res[0]),
                    "InsertedAtTimestamp": timestamp,
                    "name": str(res[1]),
                    "address": str(res[2]),
                    "cord": str(res[3]),
                    "numOfReview": str(res[4]),
                    "rating": str(res[5]),
                    "zipcode": str(res[6]),
                    "cuisine": str(res[7])

                })
        except Exception as err:
            print(">>>>>>>>" + str(err))
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }