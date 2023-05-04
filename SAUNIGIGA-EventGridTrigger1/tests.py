import azure.functions as func
from azure.storage.blob import BlobServiceClient

from function_app import main


def test_file_added_to_unprocessed_folder(mocker):
    slack_mock = mocker.patch('WorkFlowAutomation.function_app.send_slack_message', return_value=200)
    blob_data = "school_id,lat,lon\n1234,4.32,5.13"
    blob_path = "itu/unprocessed/RWA_unprocessed.csv"

    myblob = func.blob.InputStream(data=blob_data.encode('utf8'), name=blob_path)
    main(myblob)
    partner_name  = 'itu'
    file_name = 'RWA_unprocessed.csv'
    country_name = 'Rwanda'

    payload = {"text": f"File {file_name} for {country_name} has been sent to {partner_name.title()}"}
    slack_mock.assert_called_once()
    slack_mock.assert_called_with(payload=payload)

def test_file_added_to_processed_folder_one_partner_available(mocker):
    slack_mock = mocker.patch('WorkFlowAutomation.function_app.send_slack_message', return_value=200)
    partner_data_mock = mocker.patch('WorkFlowAutomation.function_app.get_partner_data', return_value=(None, None))
    blob_client_mock = mocker.patch('WorkFlowAutomation.function_app.create_blob_client', return_value ="Client")
    blob_data = "school_id,lat,lon\n1234,4.32,5.13"
    blob_path = "itu/processed/RWA_processed.csv"

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


def test_file_added_to_processed_all_files_available_itu_upload(mocker):
    slack_mock = mocker.patch('WorkFlowAutomation.function_app.send_slack_message', return_value=200)
    partner_data_mock = mocker.patch('WorkFlowAutomation.function_app.get_partner_data', return_value=(None, None))
    blob_client_mock = mocker.patch('WorkFlowAutomation.function_app.create_blob_client', return_value ="Client")
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