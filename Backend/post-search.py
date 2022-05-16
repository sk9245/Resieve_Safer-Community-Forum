import boto3
import json
import requests
from elasticsearch import Elasticsearch, RequestsHttpConnection
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError
import random

host = 'search-post-search-shshztb24utdmslsekxjaallti.us-east-1.es.amazonaws.com' 

es = Elasticsearch(
    http_auth = (**, **),
    hosts = [{'host': host, 'port': 443}],
    use_ssl = True,
    verify_certs = True,
    connection_class = RequestsHttpConnection
)  

def lambda_handler(event, context):
    # TODO implement
    
    post_tag = "Nature"
    
    findpost(post_tag)
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }

def findpost(post_tag):

    host = 'search-post-search-shshztb24utdmslsekxjaallti.us-east-1.es.amazonaws.com'
    index = 'post_tag'
    url = 'https://' + host + '/' + index + '/_search'

    query = {
            "size": 5,
            "query": {
                "multi_match": {
                    "query": post_tag,
                    "fields": ["post_tag"]
                }
            }
        }
        
    awsauth = (**, ***)
    headers = { "Content-Type": "application/json" }
    response = requests.get(url,auth=awsauth, headers=headers, data=json.dumps(query))
    res = response.json()
    print("response ",res)
    noOfHits = res['hits']['total']
    hits = res['hits']['hits']
    
    posts = {}
    i = 0
    for hit in hits:
        #print("hits ",hit)
        #posts.append(str(hit['_source']['user_id']), str(hit['_source']['post_title']), str(hit['_source']['post_tag']), str(hit['_source']['content_text']), str(hit['_source']['content_image']), str(hit['_source']['upvotes']), str(hit['_source']['timestamp']))
        print('hit ',hit['_source'])
        posts[i] = hit['_source']
        i= i + 1
    # ids = random.sample(posts, 3)
    # print(ids)
    # return ids
    print('Posts: ',posts)
    
