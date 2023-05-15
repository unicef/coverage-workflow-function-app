from datetime import datetime
import io

import azure.functions as func
from azure.storage.blob import BlobServiceClient
import pandas as pd
from pandas.testing import assert_series_equal
import pytest

import __init__
from __init__ import main, process_coverage_data, merge_coverage_and_master


@pytest.fixture
def unprocessed_data_url():
    blob_url = 'https://saunigiga.blob.core.windows.net/coverage-data-facebook/unprocessed/RWA_school_geolocation_unprocessed.csv'
    yield blob_url

@pytest.fixture
def processed_data_url():
    blob_url = 'https://saunigiga.blob.core.windows.net/coverage-data-facebook/processed/RWA.csv'
    yield blob_url

@pytest.fixture
def partner_event_defaults():

    event_details = {
        'data': {
            'fileUrl': '',
            'blobUrl': '',
            'fileType': 'AzureBlockBlob',
            'partitionId': '1',
            'sizeInBytes': 0,
            'eventCount': 0,
            'firstSequenceNumber': -1,
            'lastSequenceNumber': -1,
            'firstEnqueueTime': '0001-01-01T00:00:00',
            'lastEnqueueTime': '0001-01-01T00:00:00'
            },
        'topic': '/subscriptions/test/providers/Microsoft.EventHub/namespaces/test',
        'subject': 'eventhubs/test',
        'event_type': 'captureFileCreated',
        'event_time': datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
        'id': '7b11c4ce-1c34-4416-848b-1730e766f126',
        'data_version': ""
    }

    yield event_details


@pytest.fixture
def partner_event_unprocessed(unprocessed_data_url, partner_event_defaults):
    partner_event_defaults['data']['fileUrl'] = unprocessed_data_url
    partner_event_defaults['data']['blobUrl'] = unprocessed_data_url

    yield func.EventGridEvent(**partner_event_defaults)


@pytest.fixture
def partner_event_processed(processed_data_url, partner_event_defaults):
    partner_event_defaults['data']['fileUrl'] = processed_data_url
    partner_event_defaults['data']['blobUrl'] = processed_data_url

    yield func.EventGridEvent(**partner_event_defaults)


@pytest.fixture
def facebook_df():
    facebook_data = """giga_id_school,percent_2G,percent_3G,percent_4G
    aafa9d5e-7da7-4507-93e4-9f90aafd1ec5,90,40,30
    308eb4aa-0fd9-4a4c-84d7-fb2d8e97c699,80,30,0
    a428103f-8edd-45ae-8bad-17766f6fe887,10,0,5"""

    facebook_df = pd.read_csv(io.StringIO(facebook_data))
    yield facebook_df


@pytest.fixture
def itu_df():
    itu_data = """giga_id_school,2G,3G,4G
    aafa9d5e-7da7-4507-93e4-9f90aafd1ec5,1,1,1
    308eb4aa-0fd9-4a4c-84d7-fb2d8e97c699,0,1,0
    a428103f-8edd-45ae-8bad-17766f6fe887,1,1,0"""

    itu_df = pd.read_csv(io.StringIO(itu_data))
    yield itu_df


@pytest.fixture
def partner_data_dict(facebook_df, itu_df):
    yield {
        'facebook': {
            'data': facebook_df,
            'file_path': 'test-coverage-facebook/processed/RW.csv'
        },

        'itu': {
            'data': itu_df,
            'file_path': 'test-coverage-itu/processed/RWA_processed.csv'
        }
    }


@pytest.fixture
def master_df():
    master_data = """giga_id_school,school_id,name
    aafa9d5e-7da7-4507-93e4-9f90aafd1ec5,1,one
    308eb4aa-0fd9-4a4c-84d7-fb2d8e97c699,2,two
    a428103f-8edd-45ae-8bad-17766f6fe887,3,three
    a428103f-8edd-45ae-8bad-17766f6fe887,4,four"""

    master_df = pd.read_csv(io.StringIO(master_data))
    yield master_df


def test_file_added_to_unprocessed_folder(partner_event_unprocessed, mocker):
    slack_mock = mocker.patch('__init__.send_slack_message', return_value=200)
    main(partner_event_unprocessed)
    partner_name  = 'facebook'
    file_name = 'RWA_school_geolocation_unprocessed.csv'
    country_name = 'Rwanda'

    slack_text = f"File {file_name} for {country_name} has been sent to {partner_name.title()}"
    slack_mock.assert_called_once()
    slack_mock.assert_called_with(message=slack_text)


def test_file_added_to_processed_folder_one_partner_available(partner_event_processed, mocker):
    slack_mock = mocker.patch('__init__.send_slack_message', return_value=200)
    partner_data_mock = mocker.patch('__init__.get_partner_data', return_value=(None, None))
    blob_client_mock = mocker.patch('__init__.create_blob_client', return_value ="Client")
    blob_data = "school_id,lat,lon\n1234,4.32,5.13"
    blob_path = "itu/processed/RWA_processed.csv"

    main(partner_event_processed)
    partner_name  = 'facebook'
    file_name = 'RWA.csv'
    country_name = 'Rwanda'

    slack_text = f"Coverage file {file_name} for {country_name} has been received from {partner_name.title()}"
    slack_text += "\n"
    slack_text += f"Coverage files not processed. Not enough partner data. At least 2 sources required\n"
    # slack_message = {"text": slack_text}

    slack_mock.assert_called_once()
    slack_mock.assert_called_with(message=slack_text)


def test_file_added_to_processed_all_files_available(partner_event_processed, facebook_df, itu_df, partner_data_dict, master_df, mocker):
    slack_mock = mocker.patch('__init__.send_slack_message', return_value=200)
    partner_data_mock = mocker.patch('__init__.get_partner_data', return_value=(partner_data_dict, master_df))
    blob_client_mock = mocker.patch('__init__.create_blob_client', return_value ="Client")
    store_files_mock = mocker.patch('__init__.store_files', return_value=None)
    delete_data_mock = mocker.patch('__init__.delete_processed_partner_data', return_value=None)

    main(partner_event_processed)
    partner_name  = 'facebook'
    file_name = 'RWA.csv'
    country_name = 'Rwanda'

    slack_text = f"Coverage file {file_name} for {country_name} has been received from {partner_name.title()}"
    slack_text += "\n"
    slack_text += f"Coverage data has been processed and saved"

    slack_mock.assert_called_once()
    slack_mock.assert_called_with(message=slack_text)

    partner_data_mock.called_once()
    store_files_mock.called_once()
    delete_data_mock.called_once()


def test_coverage_data_creation(facebook_df, itu_df):
    coverage_df = process_coverage_data(facebook_df=facebook_df, itu_df=itu_df)
    coverage_type_series = pd.Series(data=['4G', '3G', '4G'], name='coverage_type')
    assert_series_equal(coverage_type_series, coverage_df['coverage_type'])

    coverage_availability_series = pd.Series(data=['YES', 'YES', 'YES'],name='coverage_availability')
    assert_series_equal(coverage_availability_series, coverage_df['coverage_availability'])


def test_master_coverage_merge(facebook_df, itu_df, master_df):
    coverage_df = process_coverage_data(facebook_df=facebook_df, itu_df=itu_df)
    merged_master_df = merge_coverage_and_master(master_df=master_df, coverage_df=coverage_df)

    assert merged_master_df.shape[0] == master_df.shape[0]
