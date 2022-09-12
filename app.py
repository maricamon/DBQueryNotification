#!/usr/bin/python
import os
import json
import psycopg2
from psycopg2 import sql

import boto3
import base64
from botocore.exceptions import ClientError

import pandas as pd

region_name = os.environ["AWS_REGION"]

def get_db_credentials(secret_name):

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    
    response = None
    
    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        raise e
    else:
        # Decrypts secret using the associated KMS key.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
            response = secret
        else:
            decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
            response = decoded_binary_secret
            
    return response
    
def query_db(db_credential):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(
            host=db_credential["host"],
            database=db_credential["engine"],
            user=db_credential["username"],
            password=db_credential["password"])
        
        # create a cursor
        cur = conn.cursor()
        
        # uncomment in production, used to generate dummy table
        """create table"""
        cur.execute('CREATE TABLE people (id INTEGER PRIMARY KEY, name VARCHAR(255) NOT NULL, fave_color VARCHAR(255) NOT NULL)')

        # uncomment in production, used to generate dummy data
        """insert items (2 options)"""
        cur.execute("""INSERT INTO people (id, name, fave_color) VALUES (%s,%s,%s)""", (1001, 'Mari', 'orange'))
        table_name = "people"
        cur.execute(
            sql.SQL("INSERT INTO {} VALUES (%s, %s, %s);")
            .format(sql.Identifier(table_name)), 
            [1004, "Tess", "pink"])
         
        """display items"""
        # print('PostgreSQL data')
        sql_command = 'SELECT * FROM people'
    
        # option 1, result of sql command is JSON then converts it to JSON String
        # cur.execute(sql_command)
        # response = cur.fetchall() 
        # response = json.dumps(response)

        # option 2, result of sql command is DataFrame then converts it to Tabular String
        response = pd.read_sql_query(sql_command, conn)
        response = json.loads(json.dumps(response, default=str))
        
        # close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        response = error
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')
        return response

def send_email(str_data, subject):
    
    """send sql email"""
    client = boto3.client('sns')
    snsArn = os.environ["dbReportTopic"]
    message = str_data
    
    response = client.publish(
        TopicArn = snsArn,
        Message = message,
        Subject=subject
    )
    
    return response
        
def save_to_bucket(df_data, filename):
    
    s3 = boto3.resource('s3')
    
    bucket_name = os.environ["bucketName"]
    bucket = s3.Bucket(bucket_name)
    
    tempfile = '/tmp/' + filename
    df_data.to_csv(tempfile)
    
    """upload the data to s3"""
    response = bucket.upload_file(tempfile, filename)
    
    return response
    
def lambda_handler(event, context):
    
    dbCredential = os.environ["dbCredentials"]
    db_credential = json.loads(get_db_credentials(secret_name=dbCredential))
    
    db_response = query_db(db_credential) 
    
    save_to_bucket(
        df_data = db_response,
        filename = 'dbq.txt')
        
    send_email_response = send_email(
        str_data = db_response,
        subject="DB Query"
        )
        
    return send_email_response