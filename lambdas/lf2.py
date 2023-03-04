import boto3
import sys
import os
import json
import boto3
from botocore.exceptions import ClientError
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from boto3.dynamodb.conditions import Key
from botocore.vendored import requests


# import requests

# Send email for SES
def send_email(sender_email, message):
    msg = MIMEMultipart()
    msg["Subject"] = "Here's a restaurant recommedation for you!"
    msg["From"] = "aa9279@nyu.edu"
    msg["To"] = "aa9279@nyu.edu"

    # Set message body
    body = MIMEText(message)
    msg.attach(body)

    # Convert message to string and send
    ses_client = boto3.client("ses", region_name="us-east-1")
    print("t1")
    response = ses_client.send_raw_email(
        Source="aa9279@nyu.edu",
        Destinations=["aa9279@nyu.edu"],
        RawMessage={"Data": msg.as_string()}
    )
    print(response)
    return response


# Get Restaurant from Elastic Search
def elastic_search_id(cuisine):
    print('i m here')
    headers = {'content-type': 'application/json'}
    esUrl = 'https://search-dinein-vilk4a6fytfynjg7kbe7eefbuy.us-east-1.es.amazonaws.com/_search?q=category:' + cuisine + '&size=1'
    esResponse = requests.get(esUrl, auth=("dininges", "Dining123."), headers=headers)
    # logger.debug("esResponse: {}".format(esResponse.text))
    data = json.loads(esResponse.content.decode('utf-8'))
    print(data)
    # logger.info("data: {}".format(data))
    try:
        # esData = data["hits"]["hits"]
        esData = data["hits"]["hits"][0]['_source']["id"]
        print(esData)
        # logger.info("esData: {}".format(esData))
    except KeyError:
        # logger.debug("Error extracting hits from ES response")
        print('e')


# Get Data from DynamoDB
def query_data_with_sort(es_id):
    print("test3")
    table_name = "yelp-restaurants"
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    table = dynamodb.Table(table_name)

    try:
        response = table.query(
            KeyConditionExpression=Key('Id').eq(es_id)

        )
    except botocore.exceptions.ParamValidationError as e:
        print(f"ParamValidationError: {e}")

    print(response["Items"])
    return response['Items'][0]


# Receive message from SQS
def receive_message():
    es_id = "ZWP7yq9C2bhCcLkMR9BmVw"
    sqs_client = boto3.client("sqs", region_name="us-east-1")
    response = sqs_client.receive_message(
        QueueUrl="https://sqs.us-east-1.amazonaws.com/658960012993/sqsq",
        MaxNumberOfMessages=10,
        WaitTimeSeconds=10,
    )

    for message in response.get("Messages", []):
        # print("test")
        message_body = message["Body"]
        print(message_body)
        # print(f"Receipt Handle: {message['ReceiptHandle']}")
        indi_msg = json.loads(message['Body'])
        cuisine = indi_msg['cuisine']

        customer_phone = indi_msg['phone']
        customer_email = indi_msg['email']
        es_id = elastic_search_id(cuisine)
        es_id = "ZWP7yq9C2bhCcLkMR9BmVw"
        item_db = query_data_with_sort(es_id)

        # if 'phone' in item_db.keys():
        #     restaurant_phone = str(item_db['phone'])
        # else:
        #     restaurant_phone = 'NA'
        address = str(item_db['address'])

        Message_to_send = 'This is a restaurant suggestion for you:\n' + 'Restaurant Name: ' + item_db[
            'name'] + '\n' + 'Address: ' + address
        print(Message_to_send)
        return Message_to_send, customer_phone, customer_email


def lambda_handler(event, context):
    try:

        message_to_send, customer_phone, customer_email = receive_message()
        print(customer_email)
        response = send_email(customer_email, message_to_send)
        # response = client.publish(
        #     PhoneNumber=customer_phone,
        #     Message=message_to_send
        # )
        print(response)
    except:
        print('Error', sys.exc_info()[0])
