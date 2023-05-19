from datetime import datetime
import os
import time

import functions_framework
import pandas as pd
from google.cloud import storage
from ga_ua_api import ga_api_request_data

@functions_framework.http
def get_ga_data(request):
    """HTTP Cloud Function

    Args:
        request (flask.Request): The request object.

    Returns:
        flask.make_response: The response text, or any set of values that can be turned into a Response object using `make_response`
    """

    request_json = request.get_json(silent=True)
    if request_json is None:
        return_data = dict()
        return_data['status'] = "Failed"
        return_data['message'] = "No request payload"
        return return_data

    request_data = dict()
    request_data.update(request_json)

    # Try setting all required variables from the request payload. Fail if any are not set.
    try:
        # Set config variables for the Google Cloud Storage location of the GCP service account json that has access to the Google Analytics view
        config_file_bucket = request_data['config_files_location']['bucket']
        config_file_auth_blob = request_data['config_files_location']['auth_blob']
        # Set extract variables for the extraction template meta data
        extract_view_id = request_data['view_id']
        extract_start_date = request_data['start_date']
        extract_end_date = request_data['end_date']
        extract_metrics_list = request_data['metrics']
        extract_template_name = request_data['name']
        # Set target variables for the Google Cloud Storage location for extracted data csv file
        target_file_project = request_data['target_project']
        target_file_bucket = request_data['target_bucket']
        target_file_blob = request_data['target_bucket_folder']
    except KeyError as exc:
        print(f"A required variable was not set: {exc}")
        return_data = dict()
        return_data['status'] = "Failed"
        return_data['message'] = f"A required variable was not set: {exc}"
        return_data['request_data'] = request_data
        return return_data

    # Set all optional variables from the request payload. Set to default values if any are not set.
    extract_dimensions_list = request_data.get('dimensions', [])
    extract_view_name = request_data.get('view_name')
    extract_property_id = request_data.get('property_id')
    extract_property_name = request_data.get('property_name')
    extract_account_id =  request_data.get('account_id')
    extract_account_name = request_data.get('account_name')
    extract_template_type = request_data.get('template_type', 'Non Template')

    print('Download auth file and set GOOGLE_APPLICATION_CREDENTIALS environment variable')
    download_blob(config_file_bucket, config_file_auth_blob, '/tmp/auth.json')
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/tmp/auth.json'

    retries = 0
    retry = 3
    while retries < retry:
        if os.path.isfile('/tmp/auth.json'):
            print(f"Starting data extraction for {extract_template_name} for dates of {extract_start_date} to {extract_end_date}")
            ga_df = ga_api_request_data(None, extract_view_id, extract_start_date, extract_end_date, extract_metrics_list, extract_dimensions_list, anti_sampling = True)
            break
        time.sleep(3)
        retries += 1
    else:
        return_data = dict()
        return_data['status'] = "Failed"
        return_data['message'] = f"Unable to load auth file after {retries} retries"
        return_data['request_data'] = request_data
        return return_data

    print('Add GA meta data to GA dataframe')
    ga_df['view_id'] = extract_view_id
    ga_df['view_name'] = extract_view_name
    ga_df['property_id'] = extract_property_id
    ga_df['property_name'] = extract_property_name
    ga_df['account_id'] = extract_account_id
    ga_df['account_name'] = extract_account_name

    print('If date field exists then change date field format from YYYYMMDD to YYYY-MM-DD')
    if 'date' in ga_df.columns:
        ga_df['date'] = pd.to_datetime(ga_df['date'], format='%Y%m%d')

    print('Create csv file from GA dataframe')
    csv_file_name = f'{target_file_blob}{extract_view_id}/{extract_template_type}/{extract_template_name}/{extract_template_name}_{datetime.strptime(extract_start_date, "%Y-%m-%d").strftime("%Y%m%d")}.csv'
    csv_file_blob = ga_df.to_csv(index=False)

    upload_blob(target_file_bucket, csv_file_blob, csv_file_name, target_file_project)

    return_data = dict()
    return_data['status'] = "Success"
    return_data['message'] = "Load complete!"
    return_data['request_data'] = request_data
    return return_data

def download_blob(bucket_name, source_blob_name, destination_file_name, project_id = None):
    """Download a file from a Google Cloud Storage bucket to a local file location

    Args:
        bucket_name (string): Name of the source Google Cloud Storage bucket
        source_blob_name (string): Name of the source Google Cloud Storage bucket path location
        destination_file_name (string): Local destination path location to write the file
        project_id (string, optional): Google Cloud Platform project id where the source storage bucket exists. Defaults to None and looks for the project id in an environment variable 'GCP_PROJECT'.

    Raises:
        Exception: If the source storage bucket does not exist, then raise this exception.
    """

    if project_id is None:
        storage_client = storage.Client(project=os.environ.get('GCP_PROJECT'))
    else:
        storage_client = storage.Client(project=project_id)

    try:
        bucket = storage_client.get_bucket(bucket_name)
    except Exception as exc:
        raise Exception("Destination bucket with config file does not exist") from exc

    blob = bucket.blob(source_blob_name)

    blob.download_to_filename(destination_file_name)

    print(f"Blob {source_blob_name} downloaded to {destination_file_name}")

def upload_blob(bucket_name, source_file_name, destination_blob_name, project_id = None):
    """Upload a string to a Google Cloud Storage bucket

    Args:
        bucket_name (string): Name of the destination Google Cloud Storage bucket
        source_file_name (string): The source string to upload
        destination_blob_name (string): Name of the destination Google Cloud Storage bucket path location
        project_id (string, optional): Google Cloud Platform project id where the destination bucket exists. Defaults to None and looks for the project id in an environment variable 'GCP_PROJECT'. Defaults to None.

    Raises:
        Exception: If the destination storage bucket does not exist, then raise this exception.
    """
    if project_id is None:
        storage_client = storage.Client(project=os.environ.get('GCP_PROJECT'))
    else:
        storage_client = storage.Client(project=project_id)

    try:
        bucket = storage_client.get_bucket(bucket_name)
    except Exception as exc:
        raise Exception("Destination bucket for file upload does not exist") from exc

    blob = bucket.blob(destination_blob_name)

    blob.upload_from_string(source_file_name, content_type='text/csv')

    print(f"File uploaded to gs://{bucket_name}/{destination_blob_name}")
