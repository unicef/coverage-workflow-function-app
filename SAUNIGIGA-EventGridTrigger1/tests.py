import azure.functions as func
from azure.storage.blob import BlobServiceClient

import pytest
from unittest.mock import Mock

import __init__
from __init__ import main


@pytest.fixture
def unprocessed_data():
    blob_url = 'https://blob.core.windows.net/coverage-data-facebook/unprocessed/RWA_school_geolocation_unprocessed.csv'
    yield blob_url

@pytest.fixture
def unprocessed_data():
    blob_url = 'https://blob.core.windows.net/coverage-data-facebook/unprocessed/RWA_school_geolocation_unprocessed.csv'
    yield blob_url


@pytest.fixture
def partner_event():
    data = {
    'fileUrl': 'https://saunigiga.blob.core.windows.net/coverage-data-facebook/unprocessed/RWA_school_geolocation_unprocessed.csv',
    'blobUrl': 'https://saunigiga.blob.core.windows.net/coverage-data-facebook/unprocessed/RWA_school_geolocation_unprocessed.csv',
    'fileType': 'AzureBlockBlob',
    'partitionId': '1',
    'sizeInBytes': 0,
    'eventCount': 0,
    'firstSequenceNumber': -1,
    'lastSequenceNumber': -1,
    'firstEnqueueTime': '0001-01-01T00:00:00',
    'lastEnqueueTime': '0001-01-01T00:00:00'
    }
    topic = '/subscriptions/5b4b650e-28b9-4790-b3ab-ddbd88d727c4/resourcegroups/test/providers/Microsoft.EventHub/namespaces/test',
    subject = 'eventhubs/test'
    event_type = 'captureFileCreated'
    event_time = '2017-07-14T23:10:27.7689666Z'
    id = '7b11c4ce-1c34-4416-848b-1730e766f126'
    data_version = ""

    return func.EventGridEvent(data=data, id=id, topic=topic, subject=subject, event_time=event_time, event_type=event_type, data_version=data_version)


def test_file_added_to_unprocessed_folder(partner_event, mocker):
    slack_mock = mocker.patch('__init__.send_slack_message', return_value=200)
    main(partner_event)
    partner_name  = 'facebook'
    file_name = 'RWA_school_geolocation_unprocessed.csv'
    country_name = 'Rwanda'

    slack_text = f"File {file_name} for {country_name} has been sent to {partner_name.title()}"
    slack_mock.assert_called_once()
    slack_mock.assert_called_with(message=slack_text)


def test_file_added_to_processed_folder_one_partner_available(partner_event, mocker):
    slack_mock = mocker.patch('__init__.send_slack_message', return_value=200)
    partner_data_mock = mocker.patch('__init__.get_partner_data', return_value=(None, None))
    blob_client_mock = mocker.patch('__init__.create_blob_client', return_value ="Client")
    blob_data = "school_id,lat,lon\n1234,4.32,5.13"
    blob_path = "itu/processed/RWA_processed.csv"

    # myblob = func.blob.InputStream(data=blob_data.encode('utf8'), name=blob_path)
    main(partner_event)
    partner_name  = 'itu'
    file_name = 'RWA_processed.csv'
    country_name = 'Rwanda'

    slack_text = f"Coverage file {file_name} for {country_name} has been received from {partner_name.title()}"
    slack_text += "\n"
    slack_text += f"Coverage files not processed. Not enough partner data. At least 2 sources required\n"
    slack_message = {"text": slack_text}

    slack_mock.assert_called_once()
    slack_mock.assert_called_with(payload=slack_message)


def test_file_added_to_processed_all_files_available_itu_upload(mocker):
    slack_mock = mocker.patch('__init__.send_slack_message', return_value=200)
    partner_data_mock = mocker.patch('__init__.get_partner_data', return_value=(None, None))
    blob_client_mock = mocker.patch('__init__.create_blob_client', return_value ="Client")
    blob_data = """giga_id_school,school_id,lat,lon
    aafa9d5e-7da7-4507-93e4-9f90aafd1ec5,1234,4.32,5.13
    308eb4aa-0fd9-4a4c-84d7-fb2d8e97c699,5678,3.23,1.456,
    a428103f-8edd-45ae-8bad-17766f6fe887,2469,1.245,4.32"""

    blob_path = "itu/processed/RWA_processed.csv"

    facebook_data = """giga_id_school,percent_2G,percent_3G,percent_4G
    aafa9d5e-7da7-4507-93e4-9f90aafd1ec5,90,40,30
    308eb4aa-0fd9-4a4c-84d7-fb2d8e97c699,5678,80,30,0
    a428103f-8edd-45ae-8bad-17766f6fe887,2469,10,0,5"""

    itu_data = """giga_id_school,2G,3G,4G
    aafa9d5e-7da7-4507-93e4-9f90aafd1ec5,1,1,1
    308eb4aa-0fd9-4a4c-84d7-fb2d8e97c699,5678,0,1,1
    a428103f-8edd-45ae-8bad-17766f6fe887,2469,1,1,0
    a428103f-8edd-45ae-8bad-17766f6fe887,2469,1,1,0"""

    myblob = func.blob.InputStream(data=blob_data.encode('utf8'), name=blob_path)
    main(myblob)
    partner_name  = 'itu'
    file_name = 'RWA_processed.csv'
    country_name = 'Rwanda'

    slack_text = f"Coverage file {file_name} for {country_name} has been received from {partner_name.title()}"
    slack_text += "\n"
    slack_text += f"Coverage files not processed. Not enough partner data. At least 2 sources required\n"
    slack_message = {"text": slack_text}

    slack_mock.assert_called_once()
    slack_mock.assert_called_with(payload=slack_message)


def test_coverage_data_creation():
    pass


def test_master_coverage_merge():
    pass

def test_save_files_after_processing():
    pass

def test_data_deletion():
    pass