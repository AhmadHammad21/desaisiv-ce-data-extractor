import requests
import json
from typing import Tuple
import uuid
from cryptography.hazmat.primitives import padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend
import hashlib
import uuid
import base64
import pickle
import redis

GS_KEY = "*De$@IA$iV*Crypt0*C0d3*"

def get_gs_key():
    return GS_KEY

def encrypt_input(input_string):
    gs_key = hashlib.md5(get_gs_key().encode('utf-8')).digest()
    input_bytes = input_string.encode('ascii')
    input_base64 = base64.b64encode(input_bytes).decode('ascii')

    padder = padding.PKCS7(8 * 8).padder()
    padded_data = padder.update(input_base64.encode('utf-8')) + padder.finalize()

    cipher = Cipher(algorithms.TripleDES(gs_key), modes.ECB(), backend=default_backend())
    encryptor = cipher.encryptor()
    encrypted_data = encryptor.update(padded_data) + encryptor.finalize()

    return base64.b64encode(encrypted_data).decode('ascii')

def fetch_bucket_user_id_report_id(sqs, event: dict, sqs_url: str):
    # Extract the records from the event
    records = event.get('Records', [])

    if not records:
        return None, None  # Handle case where there are no records


    record = records[0]
    
    receipt_handle = record.get('receiptHandle', '')
    body = json.loads(record['body'])
    
    bucket = body.get('bucket')
    file_key = body.get('file_key')

    # Delete the message after processing
    try:
        if receipt_handle:
            sqs.delete_message(
                QueueUrl=sqs_url,
                ReceiptHandle=receipt_handle
            )
            print("Deleted SQS Message")
    except Exception as e:
        print(f"Failed to delete sqs event error: {str(e)}")

    return bucket, file_key

def get_insurance_companies_names(api_url: str) -> Tuple[dict, dict]:
    """
    this function fetchs insurance companies from back-end
    returing a dictionary with insruance companies

    :param url: insruance companies API URL

    :return: two dictionaries

    Example:
        {
            1: 'Bupa',
            2: 'Arabian Shield',
        },
        {
            1: 'بوبا',
            2: 'الدرع العربي',
        }
        and
        {
            'Bupa': 'بوبا',
            'Arabian Shield': 'الدرع العربي'
        }
    """
    print("get_insurance_companies_names API")

    url = f"{api_url}/v2/Lookup/InsuranceComapnies"

    response = requests.get(url)

    if response.status_code != 200:
        raise Exception("Error when calling Insurance Companies API..")

    companies_meta_data_dict = json.loads(response.text)['data']

    # making a dictionary with id and English name and making a dictionary with id and Arabic name
    english_company_names_dict = {company_meta_data['id']: company_meta_data['englishName'] for company_meta_data in companies_meta_data_dict}
    arabic_company_names_dict = {company_meta_data['id']: company_meta_data['arabicName'] for company_meta_data in companies_meta_data_dict}

    english_arabic_companies_dict = {company_meta_data['englishName']: company_meta_data['arabicName'] for company_meta_data in companies_meta_data_dict}

    return english_company_names_dict, arabic_company_names_dict, english_arabic_companies_dict

def sqs_update_status(sqs, queue_url, user_id, report_id, status_id,
                      error_message="", stacktrace="", missing_information_data="", 
                      missing_information_message={}):

    # Construct the message body
    message_body = {
        "UserID": user_id,
        "SequenceID": report_id,
        "Status": status_id,
        "StatusType": 1,
        "ErrorMessage": error_message,
        "StackTrace": stacktrace,
        "DES_TK_D5": "vyyZ74QxsDD1FSqsYohscSOHdVgWxu01M1vYsOT6enDqPeG3f9adJqr+px6K4NwtnUGfEwS8nHuluKHIZEKhNx03Mvb8YtnS+DzB2erMyYP/PlCaIdDZuHa10pNSrhZmUk9E0YafJ9bypx3KqCsuv4fzg8C2GHeUeGLPVPZn7GQ=",
        "MissingInformationData": missing_information_data,
        "MissingInformationMessages": missing_information_message
    }

    encrypted_data = encrypt_input(json.dumps(message_body))
    print("encrypted: ", encrypted_data)

    # Send the message to the SQS queue
    response = sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=encrypted_data,
        MessageGroupId=f"{user_id}-{report_id}",  # Ensure sequential processing
        MessageDeduplicationId=str(uuid.uuid4())  # Ensure uniqueness
    )
    
    print("Response")
    print(response)

    if response['ResponseMetadata']['HTTPStatusCode'] != 200:
        raise Exception("Error when calling Status API....")
    
def files_testing_update_status(api_url, insurance_company_id, key, is_success, insurance_status_type=""):
    url = f"{api_url}/InsuranceStatus/TestTemplateStatus"
    # 1 for report status
    data = {
        "InsuranceCompanyId": insurance_company_id,
        "Key": key,
        "IsSuccess": is_success,
        "Type": insurance_status_type,
        "DES_TK_D5": "vyyZ74QxsDD1FSqsYohscSOHdVgWxu01M1vYsOT6enDqPeG3f9adJqr+px6K4NwtnUGfEwS8nHuluKHIZEKhNx03Mvb8YtnS+DzB2erMyYP/PlCaIdDZuHa10pNSrhZmUk9E0YafJ9bypx3KqCsuv4fzg8C2GHeUeGLPVPZn7GQ=",
    }

    print(type(json.dumps(data)))
    headers = {'Content-type': 'application/json', 'Accept': 'text/plain', 'Language': "1"}

    encrypted_data = encrypt_input(json.dumps(data))
    print("files_testing_update_status")
    print("encrypyed data", encrypted_data)
    resp = requests.post(url=url, data=f'"{encrypted_data}"', headers=headers)
    print(resp)

    if resp.status_code != 200:
        raise Exception("Error when calling the TestTemplateStatus API...")
    
def redis_update_status(redis_client, key, file_name, new_status,
                        claims_df, benefits_df, providers_df,
                        table_of_benefits, insurance_company, company_notes,
                        error_message, stacktrace):

    with redis_client.pipeline() as pipe:
        while True:
            try:
                # Watch the key
                pipe.watch(key)

                # Retrieve the serialized data from Redis
                data_serialized = pipe.get(key)
                if data_serialized is None:
                    # If no data is found, initialize an empty list
                    data = []
                else:
                    # Deserialize the data
                    data = pickle.loads(data_serialized)
                
                # Update the status and DataFrames
                for item in data:
                    if item.get('file') == file_name:
                        print("found file in redis_update_status")
                        print(file_name)
                        item['status'] = new_status
                        item['error_message'] = error_message
                        item['stacktrace'] = stacktrace
                        item['claims_df'] = pickle.dumps(claims_df)
                        item['benefits_df'] = pickle.dumps(benefits_df)
                        item['providers_df'] = pickle.dumps(providers_df)
                        item['table_of_benefits'] = pickle.dumps(table_of_benefits)
                        item['metadata'] = []
                        item['insurance_company'] = insurance_company
                        item['company_notes'] = company_notes

                # Start the transaction
                pipe.multi()

                # Serialize the entire list of dictionaries
                data_serialized = pickle.dumps(data)

                # Set the updated data in Redis
                pipe.set(key, data_serialized)

                # Execute the transaction
                pipe.execute()
                break  # Break the loop if successful

            except redis.WatchError:
                # If another process modified the list, retry
                continue

