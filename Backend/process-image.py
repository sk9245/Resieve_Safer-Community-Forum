import json
import boto3
import base64

s3_client = boto3.client(
    's3',
    aws_access_key_id=**,
    aws_secret_access_key=**
)

s3_res = boto3.resource(
    's3',
    aws_access_key_id=**,
    aws_secret_access_key=**
)

def moderate_image(photo, bucket):

    client=boto3.client('rekognition')

    response = client.detect_moderation_labels(Image={'S3Object':{'Bucket':bucket,'Name':photo}})

    print('Detected labels for ' + photo)    
    for label in response['ModerationLabels']:
        print (label['Name'] + ' : ' + str(label['Confidence']))
        print (label['ParentName'])
    return len(response['ModerationLabels'])

def lambda_handler(event, context):
    # TODO implement
    image_base64 = event['body']
    content_type = event['headers']['content-type']
    file_name_with_extention = event['headers']['filename']
    obj = s3_res.Object('resieve-image',file_name_with_extention)
    obj.put(Body=base64.b64decode(image_base64), ContentType=content_type)
    
    # for record in event['Records']:
        
    #     image_name = record["s3"]["object"]["key"]
    bucket='resieve-image'
    label_count=moderate_image(file_name_with_extention, bucket)
    
    resp_dict = {}
    resp_dict['isValid'] = 0
    if label_count==0:
        resp_dict['isValid'] = 1
        
    print("Labels detected: " + str(label_count))
    
    # print("EVENT ======>>>>>> ", event)
    
    return {
        "headers": {"Access-Control-Allow-Origin": "*"},
        'statusCode': 200,
        'body': json.dumps(resp_dict)
    }
    
