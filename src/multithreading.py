import pickle
import pandas as pd
import time
import json
import boto3
from .logger import logging
# from brokercecore.utils.missing_classes_utils import transform_dataframe_class
# from .validation_extractor import ValidationExtractor


class MultiThreading:
    def __init__(self, redis_client, user_id: str, report_id: str,
                 redis_key, config, pricing_offer, claim_experience_files,
                 final_data, active_list, english_companies_names,
                 arabic_companies_names, english_arabic_companies_dict) -> None:
        self.redis_client = redis_client
        self.user_id = user_id
        self.report_id = report_id
        self.redis_key = redis_key
        self.config = config
        self.sleep_time = 1  # seconds
        self.sleep_time_while = 0.5  # seconds
        self.files_count = 0
        self.data_per_company = {}
        self.pricing_offer = pricing_offer
        self.claim_experience_files = claim_experience_files
        self.final_data = final_data
        self.active_list = active_list
        self.english_companies_names = english_companies_names
        self.arabic_companies_names = arabic_companies_names
        self.english_arabic_companies_dict = english_arabic_companies_dict
        self.files_data = {} # stores file: insurance company
        self.company_notes = { # for company notes
            'English': [],
            'Arabic': []
        }
        self.duplicate_notes = { # for duplicate notes
            'English': [],
            'Arabic': []
        }
        self.failed_files = []
        self.failed_files_names_stacktrace = {} # storring failed files with their stacktraces
        self.s3_client = boto3.client('s3')
        self.sqs_client = boto3.client('sqs')

    def process(self):

        self.set_redis_key()

        self.set_redis_key_insurance_api()

        self.upload_files_temp_s3()

        self.send_sqs_messages()

        self.sleep()

        self.while_loop()

        final_data, general_missing_classes = self.aggregate_processes_data(self.final_data,
                                                                            self.active_list)

        self.remove_redis_key(self.redis_key)

        self.remove_redis_key(self.insruance_api_key)

        self.remove_s3_temp_files()

        return final_data, general_missing_classes, self.files_data, self.company_notes, self.duplicate_notes

    def set_redis_key(self) -> None:
        """Initialize redis key"""
        files_initilization = [{
            'file': file,
            'status': '',  # empty status as initialization
            'error_message': '', # storing error message
            'stacktrace': '',
            'claims_df': '',
            'benefits_df': '',
            'providers_df': '',
            'table_of_benefits':'',
            'metadata': {},
            'insurance_company': '',
            'company_notes': {}
        } for file in self.claim_experience_files]

        self.files_count = len(self.claim_experience_files)
        self._update_redis(files_initilization)

    def set_redis_key_insurance_api(self) -> None:

        insurance_list = [
            self.english_companies_names,
            self.arabic_companies_names,
            self.english_arabic_companies_dict
        ]

        self.insruance_api_key = self.redis_key + "/insurance_api"
        self.redis_client.set(self.insruance_api_key, json.dumps(insurance_list))

    def _update_redis(self, data):
        """Update Redis key with serialized data"""
        try:
            data_serialized = pickle.dumps(data)
            self.redis_client.set(self.redis_key, data_serialized)
            logging.info("Update Redis Key")
        except Exception as e:
            logging.error(f"Failed to update Redis: {e}")

    def _retrieve_from_redis(self):
        """Retrieve and deserialize data from Redis"""
        try:
            data_serialized = self.redis_client.get(self.redis_key)
            return pickle.loads(data_serialized) if data_serialized else []
        except Exception as e:
            logging.error(f"Failed to retrieve from Redis: {e}")
            return []
        
    def sleep(self):
        """Sleep for 5 seconds"""
        time.sleep(self.sleep_time)

    def is_all_processes_finished(self):
        data = self._retrieve_from_redis()
        failed_files = [item for item in data if item['status'] == 'failed']
        failed_files_names_stacktrace = {item['file']: item['stacktrace'] for item in data if item['status'] == 'failed'}
        finished_files_count = sum(1 for item in data if item['status'])  # Count all non-empty statuses
        unprocessed = [item['file'] for item in data if not item['status']]
        logging.warning(f"Length of Unprocessed files: {len(unprocessed)}")
        logging.warning(f"Unprocessed files: {unprocessed}")
        return failed_files, finished_files_count, failed_files_names_stacktrace

    def while_loop(self) -> list:
        """Keep checking until all files have been processed"""
        failed_files, finished_files_count, failed_files_names_stacktrace = self.is_all_processes_finished()
        while finished_files_count != self.files_count:
            time.sleep(self.sleep_time_while)
            failed_files, finished_files_count, failed_files_names_stacktrace = self.is_all_processes_finished()
            logging.info(f"Finished Files: {finished_files_count}/{self.files_count}")


        self.failed_files = failed_files
        self.failed_files_names_stacktrace = failed_files_names_stacktrace
        logging.info("Finished while loop - All files finished processing")

    def retrieve_data_per_file(self):
        data = self._retrieve_from_redis()
        for item in data:
            file = item['file']
            file_name = file.split("/")[-1]

            # means it's not a failed file, we don't collect the data for failed files
            if file not in self.failed_files_names_stacktrace.keys():
                insurance_company = item['insurance_company']
                file_data = self.data_per_company.get(insurance_company, {'claims': [],
                                                                    'benefits': [],
                                                                    'providers': [],
                                                                    'table_of_benefits':[],
                                                                    'metadata': {}})
                file_data['claims'].append(pickle.loads(item['claims_df']))
                file_data['benefits'].append(pickle.loads(item['benefits_df']))
                file_data['providers'].append(pickle.loads(item['providers_df']))
                file_data['table_of_benefits'].append(pickle.loads(item['table_of_benefits']))
                file_data['metadata'].update(item['metadata'])

                # updating the dictionary
                self.data_per_company[insurance_company] = file_data

                # for company check name
                self.files_data[file_name] = insurance_company
                self.company_notes['English'].extend(item['company_notes']['English'])
                self.company_notes['Arabic'].extend(item['company_notes']['Arabic'])

        logging.info("Finished retrieve_data_per_file method")
        
    def aggregate_processes_data(self, final_data, active_list):
        self.retrieve_data_per_file()
        general_missing_classes = {}
        
        validation_extractor_instance = ""#ValidationExtractor()
        
        company_duplicates = {'English':[],'Arabic':[]}
        for company, data in self.data_per_company.items():
            claims_df = pd.concat(data['claims']).reset_index(drop=True)
            benefits_df = pd.concat(data['benefits']).reset_index(drop=True)
            providers_df = pd.concat(data['providers']).reset_index(drop=True)
            table_of_benefits = data['table_of_benefits']
            metadata = data['metadata']
            
            # remove duplicates
            claims_df, benefits_df, providers_df, company_duplicates = validation_extractor_instance.remove_ce_duplicates(claims_df, benefits_df,
                                                                                                                  providers_df, company_duplicates,
                                                                                                                  self.english_companies_names[int(company)],
                                                                                                                  self.arabic_companies_names[int(company)]) 

            # original flow
            # we added this condition to make sure that original flow works
            # not satisfying in case we want only to use only OCR (PDF to Excel Feature)
            claims_df_offer = pd.DataFrame()
            benefits_df_offer = pd.DataFrame()
            providers_df_offer = pd.DataFrame()
            missing_metadata_offer = {}
            if active_list.shape[0] > 0: 
                # Generate different claim experience dataframe versions for conversion to excel in case of pricing offer 
                if self.pricing_offer:
                    claims_df_offer, benefits_df_offer, providers_df_offer = validation_extractor_instance.clean_class(claims_df.copy(),benefits_df.copy(),providers_df.copy())
                    missing_metadata_offer = {}#validation_extractor_instance.return_missing_metadata_dict(claims_df_offer)
                claims_df, benefits_df, providers_df, table_of_benefits,missing_metadata = validation_extractor_instance.process(claims_df, benefits_df, providers_df,table_of_benefits)
                # claims_df, benefits_df, providers_df, missing_classes = transform_dataframe_class(claims_df, benefits_df, providers_df, active_list)
                claims_df, benefits_df, providers_df, missing_classes = ""#transform_dataframe_class(claims_df, benefits_df, providers_df, active_list)
            else:
                claims_df, benefits_df, providers_df = validation_extractor_instance.clean_class(claims_df, benefits_df, providers_df)
                # Empty dictionary means we won't trigger missing information for underwriter 
                missing_metadata = {}#validation_extractor_instance.return_missing_metadata_dict(claims_df)
                missing_classes = {}

            general_missing_classes[company] = missing_classes
            
            self.data_per_company[company].update({
                'claims': claims_df,
                'benefits': benefits_df,
                'providers': providers_df,
                'claims_offer':claims_df_offer,
                'benefits_offer':benefits_df_offer,
                'providers_offer':providers_df_offer,
                'table_of_benefits':table_of_benefits,
                'metadata': missing_metadata,
                'metadata_offer':missing_metadata_offer,
                'classes': list(claims_df['class'].unique())
            })
        
        # Duplication message
        if len(company_duplicates['English']) == 1:
            self.duplicate_notes['English'].append('@@DuplicateNote@@')
            self.duplicate_notes['Arabic'].append('ملاحظة التكرار')
            company_duplicates_str_en = company_duplicates['English'][0]
            company_duplicates_str_ar = company_duplicates['Arabic'][0]
            self.duplicate_notes['English'].append(f"Please be aware that there has been a duplication in the uploaded claims experience of {company_duplicates_str_en} company. We have excluded it from our report.")
            self.duplicate_notes['Arabic'].append(f"يرجى العلم بوجود تكرار في سجل الخسائر المرفوع لشركة {company_duplicates_str_ar}. تم استبعاده من التقرير.")
        elif len(company_duplicates['English']) > 1:
            self.duplicate_notes['English'].append('@@DuplicateNote@@')
            self.duplicate_notes['Arabic'].append('ملاحظة التكرار')

            company_duplicates_str_en = ''
            company_duplicates_str_ar = ''
            for item in company_duplicates['English']:
                company_duplicates_str_en += item + ', '
            company_duplicates_str_en = company_duplicates_str_en[0:-2]
            for item in company_duplicates['Arabic']:
                company_duplicates_str_ar += item + '، '
            company_duplicates_str_ar = company_duplicates_str_ar[0:-2]
            self.duplicate_notes['English'].append(f"Please be aware that there has been a duplication in the uploaded claims experiences of {company_duplicates_str_en} companies. We have excluded them from our report.")
            self.duplicate_notes['Arabic'].append(f"يرجى العلم بوجود تكرار في سجلات الخسائر المرفوعة لشركات {company_duplicates_str_ar}. تم استبعادهم من التقرير.")
        
        for insur_key, values in self.data_per_company.items():
            final_data["claim_experiences"][insur_key] = values

        logging.info("aggregate_processes_data method Finished")

        return final_data, general_missing_classes
    
    def upload_files_temp_s3(self) -> None:
        """Uploading files to temporary S3 storage"""
        for file in self.claim_experience_files:
            # Generate the S3 file key and the local file path
            file_key = f"{self.user_id}/{self.report_id}/{file}"
            local_file_path = f"{self.config['download_dir']}/claim_experiences/{self.user_id}_{self.report_id}/{file}"

            # Upload file to temporary S3 bucket
            try:
                self.s3_client.upload_file(local_file_path, self.config['temp_file_s3_bucket'], file_key)
                logging.info(f"File {file} uploaded successfully to S3 bucket {self.config['temp_file_s3_bucket']}")
            except Exception as e:
                logging.error(f"Failed to upload {file} to S3: {e}")
                continue  # Skip sending SQS message if the file upload fails

        logging.info("upload_files_temp_s3 method Finished")

    def send_sqs_messages(self) -> None:
        """Send SQS messages for each file"""
        for file in self.claim_experience_files:
            # Generate the S3 file key and the local file path
            file_key = f"{self.user_id}/{self.report_id}/{file}"

            # Prepare the message body
            message_body = {
                'user_id': self.user_id,
                'report_id': self.report_id,
                'file_key': file_key,
                'bucket': self.config['temp_file_s3_bucket'],
                'single_pdf_file': True,
            }

            # Send the SQS message
            try:
                response = self.sqs_client.send_message(
                    QueueUrl=self.config['multithread_sqs_url'],
                    MessageBody=json.dumps(message_body),
                )

                logging.info(f"Message sent to SQS for {file}: {response.get('MessageId')}")
                time.sleep(1)
            except Exception as e:
                logging.error(f"Failed to send SQS message for {file}: {e}")

        logging.info("send_sqs_messages method Finished")

    def remove_redis_key(self, key):
        """Delete Key from Redis"""
        self.redis_client.delete(key)

        logging.info(f"Removed key: {key}")

        logging.info("remove_redis_key method Finished")

    def remove_s3_temp_files(self):

        folder_path = f"{self.user_id}/{self.report_id}/"
        
        response = self.s3_client.list_objects_v2(Bucket=self.config['temp_file_s3_bucket'], Prefix=folder_path)

        # Check if any objects exist under that prefix
        if 'Contents' in response:
            # Prepare list of objects to delete
            objects_to_delete = [{'Key': obj['Key']} for obj in response['Contents']]
            
            # Perform the delete operation
            delete_response = self.s3_client.delete_objects(
                Bucket=self.config['temp_file_s3_bucket'],
                Delete={
                    'Objects': objects_to_delete
                }
            )
            
            logging.info("Done remove_s3_temp_files")
        else:
            logging.info("No objects found in the specified path.")
        
