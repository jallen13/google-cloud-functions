event_data = {
    'config_files_location': {
        'bucket': 'jallen-test-bucket',
        'config_blob': 'ga_ua/ga_ua_api_extraction_config.yaml',
        'auth_blob': 'ga_ua/resolute-digital-ccfb1d08d742.json'
    },
    'date_range_criteria': {
        'start_date': '2023-01-01',
        'end_date': '2023-01-01'
    },
    'ga_account_criteria': {
        # 'view_id': '51582625', # Reformation
        'view_id': '70747477', # Resolute 
        'view_name': '1 - Production View',
        'property_id': 'UA-26305999-1',
        'property_name': 'Reformation LYMI',
        'account_id': '26305999',
        'account_name': 'Reformation LYMI'
    },
    'standard_extraction_templates': {
        'load_all': True,
        # List of standard template names to load from the config file
        'include': [
        ]
    },
    'custom_extraction_templates': {
        'load_all': False,
        'include': [
        ]
    }
}

# import os
# import main
# os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/workspaces/google-cloud-functions/ga_ua_extractions_manager/auth_files/auth.json'
# main.prep_request_batches(event_data)

# import requests
# response = requests.post("https://ga-ua-extractions-manager-6f4hxnffwq-uc.a.run.app", json=event_data)
# print(response.text)
