from datetime import datetime, timedelta
import os
import json
import calendar
import asyncio

import functions_framework
import yaml
import pandas as pd
from google.cloud import storage
import aiohttp
from aiolimiter import AsyncLimiter

@functions_framework.http
def prep_request_batches(request):
    """HTTP Cloud Function

    Args:
        request (flask.Request): The request object.

    Returns:
        flask.make_response: The response text, or any set of values that can be turned into a Response object using `make_response`
    """

    request_json = request.get_json(silent=True)
    if request_json is None:
        return "No request payload"

    request_data = dict()
    request_data.update(request_json)

    # Try setting all required variables from the request payload. Fail if any are not set.
    try:
        # Set config variables for the Google Cloud Storage location of the GCP service account json that has access to the Google Analytics view
        config_file_bucket = request_data['config_files_location']['bucket']
        config_file_auth_blob = request_data['config_files_location']['auth_blob']
        config_file_blob = request_data['config_files_location']['config_blob']
        # Set extract variables for the extraction template meta data
        extract_view_id = request_data['ga_account_criteria']['view_id']
        extract_start_date = request_data['date_range_criteria']['start_date']
        extract_end_date = request_data['date_range_criteria']['end_date']
    except KeyError as exc:
        print(f"A required variable was not set: {exc}")
        return_data = dict()
        return_data['status'] = "Failed"
        return_data['message'] = f"A required variable was not set: {exc}"
        return_data['request_data'] = request_data
        return return_data

    # Set all optional variables from the request payload. Set to default values if any are not set.
    extract_view_name = request_data.get('ga_account_criteria').get('view_name')
    extract_property_id = request_data.get('ga_account_criteria').get('property_id')
    extract_property_name = request_data.get('ga_account_criteria').get('property_name')
    extract_account_id = request_data.get('ga_account_criteria').get('account_id')
    extract_account_name = request_data.get('ga_account_criteria').get('account_name')
    extract_standard_extraction_templates_dict = request_data.get('standard_extraction_templates')
    extract_custom_extraction_templates_dict = request_data.get('custom_extraction_templates')

    print('Download auth file and set GOOGLE_APPLICATION_CREDENTIALS environment variable')
    download_blob(config_file_bucket, config_file_auth_blob, '/tmp/auth.json')
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/tmp/auth.json'

    print("Download and open config file")
    download_blob(config_file_bucket, config_file_blob, '/tmp/config.yaml')
    with open("/tmp/config.yaml", encoding="UTF-8") as config_file:
        config = yaml.safe_load(config_file)

    print('Determining which standard templates to include...')
    standard_extraction_template_list = []
    # If config include all is True then load all standard templates
    if extract_standard_extraction_templates_dict is not None and extract_standard_extraction_templates_dict.get('load_all') is True:
        standard_extraction_template_list = config['data']['standard_extraction_templates']
    else:
        for template in config['data']['standard_extraction_templates']:
            if extract_standard_extraction_templates_dict is not None and extract_standard_extraction_templates_dict.get('include') is not None:
                if template['name'] in extract_standard_extraction_templates_dict.get('include'):
                    standard_extraction_template_list.append(template)
    if standard_extraction_template_list != []:
        print(f'Standard templates to load: {str([sub["name"] for sub in standard_extraction_template_list])}')
    else:
        print('No standard templates to load')

    print('Determining which custom templates to include...')
    custom_extraction_template_list = []
    # If config include all is True then load all custom templates
    if extract_custom_extraction_templates_dict is not None and extract_custom_extraction_templates_dict.get('load_all') is True:
        custom_extraction_template_list = config['data']['custom_extraction_templates']
    else:
        for template in config['data']['custom_extraction_templates']:
            if extract_custom_extraction_templates_dict is not None and extract_custom_extraction_templates_dict.get('include') is not None:
                if template['name'] in extract_custom_extraction_templates_dict.get('include'):
                    custom_extraction_template_list.append(template)
    if custom_extraction_template_list != []:
        print(f'Custom templates to load: {str([sub["name"] for sub in custom_extraction_template_list])}')
    else:
        print('No custom templates to load')

    print('Start building the new event data list...')
    # Build the general request configs that are applicable to each request
    general_request_configs = dict()
    general_request_configs['view_id'] = extract_view_id
    general_request_configs['view_name'] = extract_view_name
    general_request_configs['property_id'] = extract_property_id
    general_request_configs['property_name'] = extract_property_name
    general_request_configs['account_id'] = extract_account_id
    general_request_configs['account_name'] = extract_account_name
    general_request_configs['target_project'] = config['target_project']
    general_request_configs['target_bucket'] = config['target_bucket']
    general_request_configs['target_bucket_folder'] = config['target_bucket_folder']
    general_request_configs['config_files_location'] = {}
    general_request_configs['config_files_location']['bucket'] = config_file_bucket
    general_request_configs['config_files_location']['auth_blob'] = config_file_auth_blob

    # Update each standard template with general configs and target bucket folder location
    if standard_extraction_template_list is not None or standard_extraction_template_list != []:
        for template in standard_extraction_template_list:
            template.update(general_request_configs)
            template['template_type'] = 'standard_templates'

    # Update each custom template with general configs and target bucket folder location
    if custom_extraction_template_list is not None or custom_extraction_template_list != []:
        for template in custom_extraction_template_list:
            template.update(general_request_configs)
            template['template_type'] = 'custom_templates'

    # Combine the standard and custom template lists
    combined_template_list = standard_extraction_template_list + custom_extraction_template_list

    # Check if the request data included any templates. If not then fail.
    if combined_template_list is None or len(combined_template_list) == 0:
        print("The request data did not include any templates to load or the requested templates are not valid templates")
        return_data = dict()
        return_data['status'] = "Failed"
        return_data['message'] = "The request data did not include any templates to load or the requested templates are not valid templates"
        return_data['request_data'] = request_data
        return return_data

    # For each template, based on the start date, end date, and batch frequency, split each template into multiple date range requests
    for template in combined_template_list:
        print(f'Determine date batches for {template["name"]}')
        if template.get('batch_freq') is None:
            batch_freq = None
        else:
            batch_freq = template['batch_freq']
        batch_date_list = ga_api_split_request_date_batch_freq(extract_start_date, extract_end_date, batch_freq)
        print(f'{len(batch_date_list)} date batches needed for {template["name"]}')
        template['date_range_batches'] = batch_date_list

    payload_list = []
    for template in combined_template_list:
        for date_range in template['date_range_batches']:
            payload = dict()
            payload.update(template)
            payload.pop('date_range_batches')
            payload['start_date'] = date_range['start_date']
            payload['end_date'] = date_range['end_date']
            payload_list.append(payload)

    print(f"Sending a total of [{len(payload_list)}] payloads to the next function")
    results = asyncio.run(async_requests(payload_list))

    results['status'] = "Success!"
    print(f"Data load attempts: {results['data_load_attempts']}")
    print(f"Data load succeses: {results['data_load_sucesses']}")
    print(f"Data load failures: {results['data_load_failures']}")
    return results

async def async_requests(payload_list):
    """Send each payload from the payload list to the next function using concurrent requests

    Args:
        payload_list (list): A list of payloads to send to the next function

    Returns:
        dictionary: A dictionary of the results from the attempted concurrent requests
    """
    successful_data_loads = []
    failed_data_loads = []
    connector = aiohttp.TCPConnector(limit=8)
    timeout = aiohttp.ClientTimeout(total=None, connect=300)
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        try:
            task_results = []
            async with asyncio.TaskGroup() as tg:
                for payload in payload_list:
                    task = tg.create_task(aiohttp_request_to_function(session, payload))
                    task.add_done_callback(task_results.append)
            for task_result in task_results:
                result = task_result.result()
                if result is None:
                    failed_data_loads.append(result)
                result_json = json.loads(result)
                if result_json['status'] == "Success":
                    successful_data_loads.append(result)
                else:
                    failed_data_loads.append(result_json)
        except ExceptionGroup as excg:
            print(excg.exceptions)

    return_dict = dict()
    return_dict['data_load_attempts'] = len(payload_list)
    return_dict['data_load_sucesses'] = len(successful_data_loads)
    return_dict['data_load_failures'] = len(failed_data_loads)
    return return_dict

async def aiohttp_request_to_function(session, payload):
    """Sends a http post request to the next function.

    Args:
        session (aiohttp.ClientSession): An active aiohttp client session for making requests
        payload (dictionary): The dictionary payload to send in the http request

    Returns:
        dictionary: On success, a dictionary of the response text. On failure, a dictionary of why the request failed.
    """
    rate_limit = AsyncLimiter(600, 60)
    async with rate_limit:
        response_status = False
        retries = 0
        retry = 3
        while response_status is False and retries < retry:
            try:
                response = await session.post("https://ga-ua-extract-data-6f4hxnffwq-uc.a.run.app", json=payload)
                print(f"Sent a payload to the next function and received a response: {payload['name']} for dates of {payload['start_date']} to {payload['end_date']}")
                response_status = response.ok
                if response_status:
                    response_text = await response.text()
                    print(f"Data successfully loaded for: {payload['name']} for dates of {payload['start_date']} to {payload['end_date']}")
                    return response_text
                print(f"Repsonse status code is {response.status}. Waiting 1 second before retrying request for: {payload['name']} for dates of {payload['start_date']} to {payload['end_date']}")
                await asyncio.sleep(1)
                retries += 1
            except aiohttp.ClientConnectionError as exc:
                print(f"Exception: {exc}. Waiting 1 second before retrying request for: {payload['name']} for dates of {payload['start_date']} to {payload['end_date']}")
                await asyncio.sleep(1)
                retries += 1
        print(f"Data failed to load for : {payload['name']} for dates of {payload['start_date']} to {payload['end_date']}")
        response_text = dict()
        response_text['status'] = "Failed"
        response_text['message'] = f"Unable to load data after {retries} retries"
        response_text['request_data'] = payload
        response_text['response_status'] = response.status
        return response_text

def ga_api_split_request_date_batch_freq(start_date, end_date, batch_freq = None):
    """Takes a start date, end date and batch frequency and returns a list of dictionaries chunnked by the batch frequency. If no batch frequency is set then returns a list of a single dictionary with the full date range.

    Args:
        start_date (string): Start date in the format of YYYY-MM-DD
        end_date (string): Start date in the format of YYYY-MM-DD
        batch_freq (string, optional): Accepts 'daily', 'weekly', or 'monthly values and chunks the date range by this. Defaults to None.

    Returns:
        _type_: _description_
    """
    start_date_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_date_dt = datetime.strptime(end_date, "%Y-%m-%d")
    batch_date_list = []
    if batch_freq is None:
        batch_dict = {}
        batch_dict['start_date'] = start_date_dt.strftime("%Y-%m-%d")
        batch_dict['end_date'] = end_date_dt.strftime("%Y-%m-%d")
        batch_date_list.append(batch_dict)
    elif batch_freq == "daily":
        date_range_list = pd.date_range(start_date, end_date, freq="D").tolist()
        for date in date_range_list:
            batch_dict = {}
            batch_dict['start_date'] = date.strftime("%Y-%m-%d")
            batch_dict['end_date'] = date.strftime("%Y-%m-%d")
            batch_date_list.append(batch_dict)
    elif batch_freq == "weekly":
        # Check if start date is the first day (Sunday) of a week. If not, then force to the first day of the week
        if start_date_dt.isoweekday() != 7:
            new_start_date = (start_date_dt - timedelta(days=((start_date_dt.isoweekday()) % 7))).strftime("%Y-%m-%d")
        else:
            new_start_date = start_date
        # Check if end date is the last day (Saturday) of a week. If not, then force to the last day of the week
        if end_date_dt.isoweekday() != 6:
            new_end_date = (end_date_dt + timedelta(days=((13 - end_date_dt.isoweekday()) % 7))).strftime("%Y-%m-%d")
        else:
            new_end_date = end_date

        date_range_list = pd.date_range(new_start_date, new_end_date, freq="W").tolist()
        for date in date_range_list:
            batch_dict = {}
            batch_dict['start_date'] = date.strftime("%Y-%m-%d")
            batch_end_date_dt = (date + timedelta(days=((13 - date.isoweekday()) % 7)))
            batch_dict['end_date'] = batch_end_date_dt.strftime("%Y-%m-%d")
            batch_date_list.append(batch_dict)
    elif batch_freq == "monthly":
        # Check if start date is the first day of a month. If not, then force to the first day of the month
        if start_date_dt.day != 1:
            new_start_date = start_date_dt.replace(day=1).strftime("%Y-%m-%d")
        else:
            new_start_date = start_date
        # Check if start date is the last day of a month. If not, then force to the last day of the month
        end_date_month_last_day_dt = datetime(end_date_dt.year, end_date_dt.month, calendar.monthrange(end_date_dt.year, end_date_dt.month)[1])
        if end_date_dt != end_date_month_last_day_dt:
            new_end_date = end_date_month_last_day_dt.strftime("%Y-%m-%d")
        else:
            new_end_date = end_date

        date_range_list = pd.date_range(new_start_date, new_end_date, freq="MS").tolist()
        for date in date_range_list:
            batch_dict = {}
            batch_dict['start_date'] = date.strftime("%Y-%m-%d")
            batch_end_date_dt = datetime(date.year, date.month, calendar.monthrange(date.year, date.month)[1])
            batch_dict['end_date'] = batch_end_date_dt.strftime("%Y-%m-%d")
            batch_date_list.append(batch_dict)

    return batch_date_list

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
