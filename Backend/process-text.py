import json
import boto3
from boto3.dynamodb.conditions import Key
import decimal
from opensearchpy import OpenSearch, RequestsHttpConnection
from elasticsearch import Elasticsearch, RequestsHttpConnection
from botocore.vendored import requests
import os
import io
import sys
import requests
# import numpy as np
# import nltk
#from hashlib import md5

ENDPOINT = os.environ['offensive_text_model']
runtime = boto3.Session().client(service_name='sagemaker-runtime',region_name='us-east-1')

aws_access_key_id = ****
aws_secret_access_key = ****

admin_email = *****

def accessDB(user):
    client = boto3.resource('dynamodb', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name='us-east-1')
    table = client.Table('user-text-posts')
    response = table.get_item(TableName='user-text-posts', Key={'user_id':user})
    print('response',response)
    count = response['Item']['count']
    print(count)
    
    if count > 3:
        return True
        
    table.put_item(
        Item={
            'user_id': user,
            'count': count+1,
            }
        )
    
    #table.update_item(Key={'user_id': user}, UpdateExpression="set Item.count = \:c", ExpressionAttributeValues={'\:c': decimal.Decimal(count+1)},ReturnValues="UPDATED_NEW")
    return False
    
def sentimentAnalysis(text, user):   
    comprehend = boto3.client(service_name='comprehend', region_name='us-east-1')
    #print('Calling DetectSentiment')
    detected_sentiment = comprehend.detect_sentiment(Text=text, LanguageCode='en')
    print("Detected Sentiment", detected_sentiment)
    #print(json.dumps(comprehend.detect_sentiment(Text=text, LanguageCode='en'), sort_keys=True, indent=4))
    #print('End of DetectSentiment\n')
    
    sentiment_label = detected_sentiment['Sentiment']
    sentiment_score = detected_sentiment['SentimentScore']['Negative']
    
    if sentiment_score > 0.90:
        # access user from db, add count, check if count reaches 3+ then send a report through SES
        status = accessDB(user)
        if status==True:
            
            ses_client = boto3.client("ses", region_name="us-east-1")
            CHARSET = "UTF-8"
            SUBJECT = "Action Needed Regarding "+user
            BODY_TEXT = "According to your criteria, this " + user+" has 3 strikes on their sentiment score. Please take the action needed."
            response = ses_client.send_email(
            Destination={
                "ToAddresses": [
                    admin_email,
                ],
            },
            Message={
                "Body": {
                    "Text": {
                        "Charset": CHARSET,
                        "Data": BODY_TEXT,
                    }
                },
                "Subject": {
                    "Charset": CHARSET,
                    "Data": SUBJECT,
                },
            },
            Source= ****,
            )   
            print("Report sent to the admin")

def insertRecord(index_data):   
    host = 'search-post-search-shshztb24utdmslsekxjaallti.us-east-1.es.amazonaws.com'

    es = OpenSearch(
        hosts = [{'host': host, 'port': 443}],
        http_auth=(**, ***),
        use_ssl = True,
        verify_certs = True,
        connection_class = RequestsHttpConnection
    )
    
    # index_data = {
    # 'user_id': post[0],
    # 'post_title': post[1],
    # 'post_tag': post[2],
    # 'content_text': post[3],
    # 'content_image': post[4],
    # 'upvotes': post[5],
    # 'timestamp': post[6]
    # }
    
    
    print('dataObject', index_data)
    es.index(index="post_tag", doc_type="Post", id=index_data['user_id'], body=index_data, refresh=True)

    
    client = boto3.resource('dynamodb', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name='us-east-1')
    table = client.Table('forum_posts')
    response = table.put_item(Item = index_data)
    print(response)
    
def lambda_handler(event, context):
    # TODO implement
    print("EVENT HERE IN LF Process TEXT ======>>", event)
    # text = event['messages'][0]['unstructured']['text']
    user = 'user1'
    # post_content = 'AWS is good!'
    post_content = json.loads(event['body'])
    print("post content text-->>", post_content["content_text"])
    payload = {"instances" : [post_content["content_text"]]}
    print("payload LF--->>", payload)
    
    # post_data = {
    # 'user_id': 'user1',
    # 'post_title': 'AWS is changing the UI',
    # 'post_tag': 'News',
    # 'content_text': 'AWS has changed its UI two times in the past week',
    # 'content_image': 'Null',
    # 'upvotes': '4',
    # 'timestamp': '02/02/2022 12:00:00'
    # }
    
    post_data = post_content.copy()
    print("Post data here in LFF---->", post_data)
    
    response = runtime.invoke_endpoint(
        EndpointName=ENDPOINT,
        Body=json.dumps(payload),
        ContentType='application/json')
    print('Model response : ',response)
    #print(payload)
    result = json.loads(response["Body"].read().decode("utf-8"))
    print("Prediction: ",result)
    
    for prediction in result:
        print('Predicted class: {}'.format(prediction['label'][0].lstrip('__label__')))
    
    pred_class = int(prediction['label'][0].lstrip('__label__'))
    # check for offenseive words
    print("pred_class type---->",type(pred_class), "pred_class value---->", pred_class)
    resp_dict = {}
    
    
    if pred_class == 1:
        print("Text is offensive")
        resp_dict['isValid'] = 1
        
        return {
        "headers": {"Access-Control-Allow-Origin": "*"},
        'statusCode': 200,
        'body': json.dumps(resp_dict)
    }
    else:
        resp_dict['isValid'] = 0
        insertRecord(post_data)
        # sentimentAnalysis(post_content['content_text'],post_content['user_id'])
        
        # host = 'search-post-search-shshztb24utdmslsekxjaallti.us-east-1.es.amazonaws.com'
        # index = 'post_tag'
        # url = 'https://' + host + '/' + index + '/_search'
        
        # es = Elasticsearch(
        #     http_auth = (***, ***),
        #     hosts = [{'host': host, 'port': 443}],
        #     use_ssl = True,
        #     verify_certs = True,
        #     connection_class = RequestsHttpConnection
        #     ) 
        # request = '''{"query": {"match_all": {}}}'''
        # results = es.search(index="post_tag", body=request, size=100)['hits']['hits']
        
        # posts = []
        # for hit in results:
        #     #print("hits ",hit)
        #     #posts.append(str(hit['_source']['user_id']), str(hit['_source']['post_title']), str(hit['_source']['post_tag']), str(hit['_source']['content_text']), str(hit['_source']['content_image']), str(hit['_source']['upvotes']), str(hit['_source']['timestamp']))
        #     print('hit ',hit['_source'])
        #     posts.append(hit['_source'])
        # # ids = random.sample(posts, 3)
        # # print(ids)
        # # return ids
        # print('Posts: ',posts)
        
        # # print("ES RESULTS --->", results)
        # # host = 'search-post-search-shshztb24utdmslsekxjaallti.us-east-1.es.amazonaws.com'
        # # index = 'post_tag'
        # # url = 'https://' + host + '/' + index + '/_search'
    
        # # query = {
        # #         "size": 100,
        # #         "query": {
        # #             "multi_match": {
        # #                 "query": '*',
        # #                 "fields": ["post_tag"]
        # #             }
        # #         }
        # #     }
            
        # # awsauth = (***, ***)
        # # headers = { "Content-Type": "application/json" }
        # # response = requests.get(url,auth=awsauth, headers=headers, data=json.dumps(query))
        # # res = response.json()
        # # print("response ",res)
        # # noOfHits = res['hits']['total']
        # # hits = res['hits']['hits']
        
        # # posts = {}
        # # i = 0
        # # for hit in hits:
        # #     #print("hits ",hit)
        # #     #posts.append(str(hit['_source']['user_id']), str(hit['_source']['post_title']), str(hit['_source']['post_tag']), str(hit['_source']['content_text']), str(hit['_source']['content_image']), str(hit['_source']['upvotes']), str(hit['_source']['timestamp']))
        # #     print('hit ',hit['_source'])
        # #     posts[i] = hit['_source']
        # #     i= i + 1
        # # # ids = random.sample(posts, 3)
        # # # print(ids)
        # # # return ids
        # # print('Posts: ',posts)
        
        aws_access_key_id = ***
        aws_secret_access_key = ***
        
        client = boto3.resource('dynamodb', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name='us-east-1')
        table = client.Table('forum_posts')
        res = table.scan()["Items"]
    
        return {
            "headers": {"Access-Control-Allow-Origin": "*"},
            'statusCode': 200,
            'body': json.dumps({"a":res})
        }

