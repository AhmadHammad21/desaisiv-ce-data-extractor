import os
import io
import json
import boto3
import botocore
import pickle
import traceback
import zipfile
import re
import shutil
import yaml
import time
import redis
from dotenv import load_dotenv
from src.multithreading import MultiThreading
from src.files_validation import (
    is_valid_report, is_active_list, is_raw_data,
    active_list_extraction, raw_data_extraction
)
from src.apis_sqs import (
    sqs_update_status, get_insurance_companies_names, fetch_bucket_user_id_report_id,
    files_testing_update_status, redis_update_status
)

DEFAULT_ENV = "dev"
env = os.getenv('APP_ENV', DEFAULT_ENV)
cfg = botocore.config.Config(retries={'max_attempts': 0}, read_timeout=840, connect_timeout=600, region_name="us-east-1")
lambdaClient = boto3.client('lambda', config=cfg, region_name="us-east-1")
s3Client = boto3.client("s3")
# Create an SQS client
sqs = boto3.client('sqs', region_name="us-east-1")  
CONFIG_PATH = f"./config_{env}.yml"

def load_config(config_path):
    config = {}
    with open(config_path) as f:
        config =  yaml.safe_load(f)

    if not config:
        raise Exception("Issue in loading the configuration file")

    return config

def full_report_files_multi_thread_handler(bucket: str, file_key: str, config: dict):
    """
    this handle is to handle the fully results that show
    """
    try:
        print(f"Detected Environment: {env}")
        start = time.time()

        print(f"----- Template Extraction for Zip File: {bucket}/{file_key}")
        user_id, report_id, _ = file_key.split("/")
        print(f"user_id: {user_id} report_id: {report_id}")

        REDIS_DB_URL = os.environ['REDIS_DB_URL']
        PASSWORD = os.environ['PASSWORD']
        REDIS_INDEX = os.environ['REDIS_INDEX']

        zip_path = f"{config['download_dir']}/claim_experiences/{user_id}_{report_id}/"
        print("Zip Path: ", zip_path)

        # Update Status=InProgress
        sqs_update_status(
            sqs=sqs, 
            queue_url=config["sqs_url"],
            user_id=user_id,
            report_id=report_id,
            status_id=1
        )

        final_data = {"claim_experiences": {}}
        response = s3Client.get_object(Bucket=bucket, Key=file_key)
        content = io.BytesIO(response['Body'].read())
        copy_source = {'Bucket': bucket, 'Key': file_key}
        analysis_bucket = config.get('analysis_s3_bucket','')
        s3Client.copy_object(
            Bucket=analysis_bucket, Key=f'{user_id}/{report_id}/files/Data.zip', CopySource=copy_source
        )
        
        z = zipfile.ZipFile(content)
        files_list = z.namelist()
        claim_experience_files = list(filter(lambda file: is_valid_report(file), files_list))
        active_list_files = list(filter(lambda file: is_active_list(file), files_list))
        raw_data_files = list(filter(lambda file: is_raw_data(file), files_list))
        failed_files = []

        print("----- Files Found: ------")
        print(files_list)
        print(claim_experience_files)
        print(active_list_files)
        print(raw_data_files)
        print("------------------")
        z.extractall(zip_path)

        final_data = active_list_extraction(
            active_list_files=active_list_files,
            zip_path=zip_path,
            final_data=final_data
        )

        final_data = raw_data_extraction(
            raw_data_files=raw_data_files,
            zip_path=zip_path,
            final_data=final_data
        )

        english_companies_names = {}
        english_arabic_companies_dict = {}
        # Claim Experiences
        if len(claim_experience_files) >= 1:
            # this dictionary has missing information per company id
            general_missing_classes = {}

            # getting companies id: names dict and english_company_name: arabic_company_name dict
            english_companies_names, arabic_companies_names, english_arabic_companies_dict = get_insurance_companies_names(config['api_url'])

            companyNameCheckObject = CompanyNameCheck(english_companies_names, arabic_companies_names)

            redis_client = redis.Redis(host=REDIS_DB_URL, password=PASSWORD, db=REDIS_INDEX)
            print("Done Creating Redis Client")
            redis_key = f"{user_id}_{report_id}"

            multhreading = MultiThreading(
                redis_client=redis_client, user_id=user_id, report_id=report_id, redis_key=redis_key,
                config=config, claim_experience_files=claim_experience_files,
                final_data=final_data, english_companies_names=english_companies_names,
                arabic_companies_names=arabic_companies_names,
                english_arabic_companies_dict=english_arabic_companies_dict
            )

            final_data, files_data, company_notes, duplicate_notes = multhreading.process()

            failed_files = multhreading.failed_files
            failed_files_names_stacktrace = multhreading.failed_files_names_stacktrace
            print(f"Failed files: {failed_files}")
            print(f"Number of Failed files: {len(failed_files)}")
            print(f"Failed files stacktrace: {failed_files_names_stacktrace}")

            # means there are failed files
            if len(failed_files) != 0:
                failed_files_stacktrace = "\n".join(list(failed_files_names_stacktrace.values()))
                raise Exception("One or more of the PDF OCR Files failed")

            # calling the update
            if len(company_notes['English']) > 0:
                company_notes['English'] = companyNameCheckObject.add_numbers_notes(company_notes['English'], lang='en')
                company_notes['Arabic'] = companyNameCheckObject.add_numbers_notes(company_notes['Arabic'], lang='ar')
            
            if len(duplicate_notes['English']) > 0:
                company_notes['English'] = company_notes['English'] + duplicate_notes['English']
                company_notes['Arabic'] = company_notes['Arabic'] + duplicate_notes['Arabic']
                
            if len(company_notes['English']) > 0:
                update_files_data(config['update_files_data_sqs_url'], report_id, files_data, company_notes)

            # final_data, latest_company_missing_classes, metadata = map_classes(config, s3Client, user_id, report_id, final_data, general_missing_classes)

        # this else handles if we pass only raw data without claims experience
        else:
            print("Only Raw Data Detected..")
            # final_data, raw_data_missing_information = map_classes_rawdata(config, s3Client, user_id, report_id, final_data)

        response = s3Client.put_object(
            Body=pickle.dumps(final_data),
            Bucket=config["data_s3_bucket"],
            Key=f"{user_id}/{report_id}/dataframes.txt"
        )
        print(response)

        print("Final_data: ", final_data)

        end = time.time()
        print("Total Time in Seconds: " + str(end - start))
  
    except Exception as e:
        # Update Status=Failed

        # error and stacktrace should be viewed from failed files
        if len(failed_files) != 0:
            print("Stacktrace from failed files multithreading")
            stacktrace = failed_files_stacktrace
        else:
            stacktrace = traceback.format_exc()
        print(stacktrace)

        # 3: Analyzing
        sqs_update_status(
            sqs=sqs,
            queue_url=config["sqs_url"],
            user_id=user_id,
            report_id=report_id,
            status_id=3,
            error_message=str(e),
            stacktrace=f"OCR: {stacktrace}"
        )

    shutil.rmtree(zip_path, ignore_errors=True)

def single_file_ocr_handler(bucket: str, file_key: str, config: dict):
    """
    this handle is process one pdf file
    """
    try:
        print(f"Detected Environment: {env}")
        start = time.time()

        print("single_file_ocr_handler Started")
        user_id, report_id, _ = file_key.split("/")

        print(f"user_id: {user_id}, report_id: {report_id}")
        print(f"Bucket: {bucket}, file_key {file_key}")

        REDIS_DB_URL = os.environ['REDIS_DB_URL']
        PASSWORD = os.environ['PASSWORD']
        REDIS_INDEX = os.environ['REDIS_INDEX']

        local_file_path = f"{config['download_dir']}/{os.path.basename(file_key)}"

        s3Client.download_file(bucket, file_key, local_file_path)

        redis_client = redis.Redis(host=REDIS_DB_URL, password=PASSWORD, db=REDIS_INDEX)

        redis_key = f"{user_id}_{report_id}"

        # "123/4292/1/file.pdf" to "1/file.pdf"
        redis_file_name = "/".join(file_key.split("/")[-2:])

        claim_experiences = {}
        insur_company_id = int(re.sub(r"\\", "/", file_key).split("/")[-2])

        company_id = str(insur_company_id)
        claim_experiences[company_id] = [local_file_path]

        # getting companies id: names dict and english_company_name: arabic_company_name dict
        insruance_api_key = redis_key + "/insurance_api"
        insurance_data = redis_client.get(insruance_api_key)
        english_companies_names, arabic_companies_names, english_arabic_companies_dict = json.loads(insurance_data)

        companyNameCheckObject = CompanyNameCheck(english_companies_names, arabic_companies_names)

        # Check key
        company_id_from_result, company_notes = companyNameCheckObject.process(local_file_path, company_id)

        claims_df, benefits_df, providers_df, table_of_benefits = TemplateExtractorFactory().process(company_id_from_result,
                                                                                            claim_experiences[company_id],
                                                                                            f"{user_id}/{report_id}/{insur_company_id}",
                                                                                            config)

        # redis cache for output and status
        redis_update_status(redis_client, redis_key, redis_file_name, 'success', claims_df, benefits_df,
                            providers_df, table_of_benefits, company_id_from_result, company_notes, '', '')
         
        end = time.time()
        print("Total Time in Seconds: " + str(end - start))

    except Exception as e:
        # Update Status=Failed
        stacktrace = traceback.format_exc()
        print(stacktrace)

        redis_update_status(redis_client, redis_key, redis_file_name, "failed", "", "", "", "", "", company_notes, str(e), f"OCR: {stacktrace}")


def testing_files_handler(bucket: str, file_key: str, config: dict):
    """
    this is to handle testing one file
    """
    try:
        print(f"Detected Environment: {env}")
        start = time.time()

        user_id = 51
        report_id = 30

        # bucket, key = fetch_bucket_user_id_report_id(event)

        print(f"Bucket Name: {bucket} Key: {file_key}")
        print(os.listdir())

        local_file_path = f"{config['download_dir']}/{os.path.basename(file_key)}"

        # downloading the file
        s3Client.download_file(bucket, file_key, local_file_path)
        print("after downloading")
        print(os.listdir())
 
        print("------------------")

        claim_experiences = {}

        insur_company_id = int(re.sub(r"\\", "/", file_key).split("/")[0])
        company_id = str(insur_company_id)
        claim_experiences[company_id] = [local_file_path]

        claims_df, benefits_df, providers_df, table_of_benefits = TemplateExtractorFactory().process(company_id,
                                                                                            claim_experiences[company_id],
                                                                                            f"{user_id}/{report_id}/{insur_company_id}",
                                                                                            config)

        files_testing_update_status(config["api_url"], insur_company_id, file_key, True, "Claim")
        end = time.time()
        print("Total Time in Seconds: " + str(end - start))

    except Exception as e:
        # Update Status=Failed
        stacktrace = traceback.format_exc()
        print(stacktrace)

        files_testing_update_status(config["api_url"], insur_company_id, file_key, False, "Claim")

def lambda_handler(event, context):
    print(f"Detected Environment: {env}")
    print(f"Event: {event}")
    config = load_config(CONFIG_PATH)
    
    print(f"Loading Config File {CONFIG_PATH}: ", config)

    load_dotenv()

    files_testing_buckets = config.get('files_testing', '')
    temp_file_bucket = config.get('temp_file_s3_bucket', '')

    bucket, file_key = fetch_bucket_user_id_report_id(
        sqs=sqs,
        event=event,
        sqs_url=config['sqs_url']
    )

    print(f"bucket found: {bucket} file_key: {file_key}")

    if bucket in ["broker-claim-exp-report-files-dev", "broker-claim-exp-report-files-qa",
                  "broker-claim-exp-report-files-uat", "broker-claim-exp-report-files-prod"]:
        print("Full Files Pipeline..")
        # multithreading
        full_report_files_multi_thread_handler(
            bucket=bucket,
            file_key=file_key,
            config=config
        )
    # single file lambda
    elif bucket == temp_file_bucket:
        print("Single File Pipeline..")
        single_file_ocr_handler(
            bucket=bucket,
            file_key=file_key,
            config=config
        )

    # Testing files lambda
    elif bucket in files_testing_buckets:
        print("Testing File Pipeline..")
        testing_files_handler(
            bucket=bucket, 
            file_key=file_key,
            config=config
        )
    else:
        print("Double check the bucket names and")

