from datetime import datetime
from functools import reduce
import io
import json
import logging
import os
import re
import traceback
from typing import Any

import azure.functions as func
from azure.storage.blob import BlobServiceClient

import country_converter as coco
import numpy as np
import pandas as pd
import requests


def main(event: func.EventGridEvent):

    event_data = event.get_json()
    logging.info(event_data)
    blob_url = event_data['blobUrl']
    blob_file_path = re.search(r"(?<=(blob.core.windows.net\/))(.*)", blob_url)[0]

    if not blob_file_path or not str(blob_file_path).endswith('.csv'):
        logging.info(f'Uploaded file {blob_file_path} is not a valid file')
        return

    container_name, folder_name, file_name = blob_file_path.split('/')
    partner_name = container_name.replace('coverage-data-', '')
    country_code = re.split(r'[^a-zA-Z]', file_name)[0]
    country_name = coco.convert(country_code, to='name_short')

    if folder_name == "unprocessed":
        slack_text = f"File {file_name} for {country_name} has been sent to {partner_name.title()}"

        # if os.environ.get('GIGA_EMAILS'):

        #     partner_emails = os.environ[f'{partner_name.upper()}_EMAIL_LIST']
        #     giga_emails = os.environ['GIGA_EMAILS']
        #     email_list = partner_emails + giga_emails
        #     email_message = "Hello, a new file has been updated to the Giga storage folder {country_name}"
        #     subject = f"Coverage data for {country_name}"
            # send_email(recipient_list=email_list, subject=subject, body=message, )

    elif folder_name == "processed":
        slack_text = f"Coverage file {file_name} for {country_name} has been received from {partner_name.title()}"

        blob_service_client = create_blob_client()

        try:
            partners_list = ['facebook', 'itu']
            partners_data_dict, master_df = get_partner_data(blob_service_client, country_name, partners_list=partners_list)
        except Exception as e:
            error_text = f"Error while getting partner and master data for {country_name}:\n{traceback.format_exc()}"
            send_slack_message(message=error_text)
            raise

        if type(partners_data_dict) == type(None):
            slack_text += "\n"
            slack_text += f"Coverage files not processed. Not enough partner data. At least 2 sources required\n"
        
        else:
        
            try:
                facebook_df = partners_data_dict['facebook']['data']
                itu_df = partners_data_dict['itu']['data']
                coverage_df = process_coverage_data(facebook_df=facebook_df, itu_df=itu_df)
            except Exception as e:
                error_text = f"Error while processing coverage data:\n{traceback.format_exc()}"
                send_slack_message(message=error_text)
                raise

            try:
                master_with_coverage = merge_coverage_and_master(master_df=master_df, coverage_df=coverage_df)
            except Exception as e:
                error_text = f"Error while processing coverage data:\n{traceback.format_exc()}"
                send_slack_message(message=error_text)
                raise
                
            try:
                store_files(country_name=country_name, blob_service_client=blob_service_client, facebook_df=facebook_df,
                            itu_df=itu_df, coverage_df=coverage_df, master_df=master_with_coverage)
            except Exception as e:
                error_text = f"Error while saving files:\n{traceback.format_exc()}"
                send_slack_message(message=error_text)
                raise


            try:
                delete_processed_partner_data(blob_service_client=blob_service_client, partners_data_dict=partners_data_dict)
            except Exception as e:
                error_text = f"Error while deleting files:\n{traceback.format_exc()}"
                send_slack_message(message=error_text)
                raise

            slack_text += "\n"
            slack_text += f"Coverage data has been processed and saved"
    
    logging.info("Sending Slack message")
    send_slack_message(message=slack_text)
    logging.info("Slack message successfully sent")

    logging.info(f'Python Blob trigger function processed {file_name}')


def get_partner_data(blob_service_client: BlobServiceClient, country_name: str, 
                     partners_list: list[str]) -> tuple[dict, pd.DataFrame]:
    
    partners_data_dict = {}

    for partner in partners_list:
        partner_df, partner_file_path = get_blob_storage_data(blob_service_client, partner, country_name)

        if type(partner_df) == type(None):
            return None, None
        else:
            partners_data_dict[partner] = {'data': partner_df, 'file_path': partner_file_path}

    container_name = os.environ['DATA_CONTAINER_NAME']
    master_df, _ = get_blob_storage_data(blob_service_client=blob_service_client, container_name=container_name, country_name=country_name)

    return partners_data_dict, master_df


def store_files(country_name, blob_service_client, facebook_df, itu_df, coverage_df, master_df):
        container_name = os.environ['DATA_CONTAINER_NAME']
        raw_coverage_folder = os.environ['RAW_COVERAGE_FOLDER']
        processed_coverage_folder = os.environ['PROCESSED_COVERAGE_FOLDER']
        master_file_folder = os.environ['MASTER_FILE_FOLDER']

        datetime_string = datetime.today().strftime('%Y%m%d_%H%M%S')
        iso3_code = coco.convert(country_name, to='iso3')
        facebook_file_path = f'{raw_coverage_folder}/facebook/{iso3_code}_coverage_data_{datetime_string}.csv'
        itu_file_path = f'{raw_coverage_folder}/itu/{iso3_code}_coverage_data_{datetime_string}.csv'
        coverage_file_path = f'{processed_coverage_folder}/{iso3_code}_school_geolocation_coverage_master.csv'
        master_file_path = f'{master_file_folder}/{iso3_code}_school_geolocation_coverage_master.csv'

        # upload different files to respective locations
        upload_to_blob_client(blob_service_client=blob_service_client, container=container_name, blob_file_path=facebook_file_path,
                              df=facebook_df)
        upload_to_blob_client(blob_service_client=blob_service_client, container=container_name, blob_file_path=itu_file_path,
                              df=itu_df)
        upload_to_blob_client(blob_service_client=blob_service_client, container=container_name, blob_file_path=coverage_file_path,
                              df=coverage_df, overwrite=True)
        upload_to_blob_client(blob_service_client=blob_service_client, container=container_name, blob_file_path=master_file_path,
                              df=master_df, overwrite=True)


def delete_processed_partner_data(blob_service_client: BlobServiceClient, partners_data_dict: dict[dict[str, Any]]) -> None:
    for partner_name in partners_data_dict.keys():
        container_name = f"coverage-data-{partner_name}"
        file_path = partners_data_dict[partner_name]['file_path']
        delete_blob_client(blob_service_client=blob_service_client, container=container_name, blob_file_path=file_path)


def get_blob_storage_data(blob_service_client: BlobServiceClient, container_name: str, country_name: str) -> pd.DataFrame:
    if container_name == 'facebook':
        container_name = f"coverage-data-{container_name}"
        iso_code = coco.convert(country_name, to='ISO2').upper()
        name_starts_with = f'processed/{iso_code}'
    elif container_name == 'itu':
        container_name = f"coverage-data-{container_name}"
        iso_code = coco.convert(country_name, to='ISO3').lower()
        name_starts_with = f'processed/{iso_code}'
    elif container_name == 'giga':
        iso_code = coco.convert(country_name, to='ISO3').upper()
        name_starts_with = f'gold/school_data/{iso_code}'
    else:
        Exception('Invalid container name provided. Must be one of; facebook, itu and giga')

    blobs_with_name = get_list_of_blobs(blob_service_client = blob_service_client, container=container_name, name_starts_with=name_starts_with)
    blobs_with_name = [blob['name'] for blob in blobs_with_name]

    try:
        blob_name = blobs_with_name[0]
    except IndexError:
        logging.info(f'File from {container_name} for {country_name} not yet received')
        return None, None
    
    partner_data = download_from_blob_client(blob_service_client=blob_service_client, container=container_name, blob_file_path=blob_name)
    partner_df = pd.read_csv(io.BytesIO(partner_data))
    return partner_df, blob_name
    

def process_coverage_data(facebook_df: pd.DataFrame, itu_df: pd.DataFrame):

    # prepare Facebook data
    facebook_df['2G_coverage'] = (facebook_df['percent_2G'] > 0)
    facebook_df['3G_coverage'] = (facebook_df['percent_3G'] > 0)
    facebook_df['4G_coverage'] = (facebook_df['percent_4G'] > 0)

    # prepare ITU data
    itu_df['2G_coverage'] = (itu_df['2G'] >= 1)
    itu_df['3G_coverage'] = (itu_df['3G'] == 1)
    itu_df['4G_coverage'] = (itu_df['4G'] == 1)

    itu_cols_to_rename = ['Schools_within_1km','Schools_within_2km', 'Schools_within_3km', 'Schools_within_10km']
    itu_df = itu_df.rename(lambda col: col.lower() if col in itu_cols_to_rename else col, axis='columns')


    fb_cols_to_keep = ['giga_id_school', '2G_coverage', '3G_coverage', '4G_coverage']
    itu_cols_to_keep = ['giga_id_school', '2G_coverage', '3G_coverage', 'fiber_node_distance',
                        'microwave_node_distance', 'nearest_school_distance', 'schools_within_1km', 'schools_within_2km',
                        'schools_within_3km', 'schools_within_10km', 'nearest_LTE_id', 'nearest_LTE_distance',
                        'nearest_UMTS_id', 'nearest_UMTS_distance', 'nearest_GSM_id', 'nearest_GSM_distance',
                        '2G_coverage', '3G_coverage', '4G_coverage', 'pop_within_1km', 'pop_within_2km',
                        'pop_within_3km', 'pop_within_10km']
    
    facebook_df = facebook_df.loc[facebook_df['giga_id_school'].notna(), fb_cols_to_keep]
    itu_df = itu_df.loc[itu_df['giga_id_school'].notna(), [col for col in itu_cols_to_keep if col in itu_df.columns]]
    
    
    # combine the coverage data
    coverage_df = facebook_df.merge(itu_df, on='giga_id_school', suffixes=('', '_itu'), how='outer')

    # harmonize the coverage columns
    for column in ( '4G_coverage', '3G_coverage', '2G_coverage'):
        coverage_df[f'{column}'] = coverage_df[[col for col in coverage_df.columns if col.startswith(column)]].any(
            axis='columns')

    coverage_df['coverage_type'] = np.select(
        [coverage_df['4G_coverage'], coverage_df['3G_coverage'], coverage_df['2G_coverage']],
        ['4G', '3G', '2G'],
        default='no coverage'
    )

    for column in ( '2G_coverage', '3G_coverage', '4G_coverage'):
        coverage_df.loc[:, column] = coverage_df[column].map({True: 'YES', False: 'NO'})

    coverage_df['coverage_availability'] = np.where(coverage_df['coverage_type'] == 'no coverage', 'NO', 'YES')

    # drop the unnecessary columns
    coverage_df = coverage_df[[col for col in coverage_df.columns if not col.endswith('_itu')]]

    return coverage_df


def merge_coverage_and_master(master_df: pd.DataFrame, coverage_df: pd.DataFrame) -> pd.DataFrame:
    """
    Merges the processed coverage data with the school geolocation master data to create the full dataset for the
    country. It also contains all the columns required in the full master dataset even if they may be empty

    :param master_df: The dataframe of the school geolocation master data
    :param coverage_df: The dataframe of the processed coverage data
    :returns: pd.DataFrame
    """
    master_df = master_df.merge(coverage_df, how='left', on='giga_id_school')

    main_cols = ['giga_id_school', 'school_id', 'name', 'lat', 'lon', 'education_level',
                 'education_level_regional', 'school_type',
                 'connectivity', 'connectivity_speed', 'type_connectivity', 'coverage_availability', 'coverage_type',
                 'latency_connectivity', 'admin1', 'admin2', 'admin3', 'admin4', 'school_region', 'num_computers',
                 'num_teachers', 'num_students', 'num_classroom', 'computer_availability', 'computer_lab',
                 'electricity', 'water',
                 'address', 'fiber_node_distance', 'microwave_node_distance',
                 'nearest_school_distance', 'schools_within_1km', 'schools_within_2km', 'schools_within_3km',
                 'schools_within_10km', 'nearest_LTE_id', 'nearest_LTE_distance', 'nearest_UMTS_id',
                 'nearest_UMTS_distance', 'nearest_GSM_id', 'nearest_GSM_distance', 'pop_within_1km', 'pop_within_2km',
                 'pop_within_3km',
                 'pop_within_10km']

    for column in main_cols:
        if column not in master_df.columns:
            master_df[column] = np.nan

    master_df = master_df[main_cols]

    return master_df
    

def send_slack_message(message: str, webhook: str = None) -> requests.Response:
    """Send a Slack message to a channel via a webhook.
    :param payload: Dictionary containing Slack message, i.e. {"text": "This is a test"}
    :returns: HTTP response code, i.e. <Response [503]>
    """

    if not webhook:
        webhook = os.environ['SLACK_WEBHOOK']

    payload = {"text": message}

    return requests.post(webhook, json.dumps(payload))


def create_blob_client():
    connection_string = os.environ['saunigiga_STORAGE']
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    return blob_service_client


def download_from_blob_client(blob_service_client: BlobServiceClient, container, blob_file_path, local_file_path=None):

    try:
        blob_client = blob_service_client.get_blob_client(container=container, blob=blob_file_path, snapshot=None)
        data = blob_client.download_blob().readall()
    except Exception as e:
        error_text = f"Error while fetching file: {blob_file_path} from container {container}"
        send_slack_message(message=error_text)
        raise

    if not local_file_path:
        return data
    else:
        with open(local_file_path, 'wb') as f:
            f.write(data)

    return local_file_path


def upload_to_blob_client(blob_service_client: BlobServiceClient, container: str, blob_file_path: str, df: pd.DataFrame,
                          overwrite=False):
    binary_data = df.to_csv(index=False).encode()

    blob_client = blob_service_client.get_blob_client(container=container, blob=blob_file_path, snapshot=None)
    upload_response = blob_client.upload_blob(binary_data, overwrite=overwrite)
    return upload_response

def delete_blob_client(blob_service_client: BlobServiceClient, container: str, blob_file_path: str):
    blob_client = blob_service_client.get_blob_client(container=container, blob=blob_file_path, snapshot=None)
    delete_response = blob_client.delete_blob()
    return delete_response


def get_list_of_blobs(blob_service_client: BlobServiceClient, container: str, name_starts_with: str):
    container_client = blob_service_client.get_container_client(container=container)
    blob_names = container_client.list_blobs(name_starts_with=name_starts_with)
    return blob_names
