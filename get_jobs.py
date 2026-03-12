import requests

current_user = "genomefltr"
api_key = "VgpwXObaQz0xIgAAvzvn2jIQ5Jzo2ElkI8ENlbmwXAc"
SLURMAPI_BASE = "https://saw.tau.ac.il/slurmapi"
base_url = "https://saw.tau.ac.il/slurmapi/jobs/" # slurprod
generate_token_url = f"{base_url}/slurmapi/generate-token/"
slurmrestd_url = f"{base_url}/slurmrestd"
account_name = "pupkoweb-users"

def get_api_token(username, api_key):
    """
    Retrieves a JWT token for SLURM REST API access for powerslurm cluster.

    Parameters:
    username (str): The username of the user requesting the token.
    api_key (str): The API key provided by the HPC team.

    Returns:
    str: The API token if the request is successful.

    Raises:
    Exception: If the request fails with a non-200 status code.
    """

    generate_token_url = 'https://slurmtron.tau.ac.il/slurmapi/generate-token/'

    payload = {
        'username': username,
        'api_key': api_key
    }

    response = requests.post(generate_token_url, data=payload)

    if response.status_code == 200:
        # Extracting the token from the JSON response
        return response.json()['SlurmJWT']
    else:
        raise Exception(f"Error: {response.status_code}, {response.text}")


def get_jobs(account=None, cluster=None, logger=None):
    #print(f'in get_jobs()')
    #logger.info(f'in get_jobs()')
    params = {}
    if account:
        params['account'] = account
    if cluster:
        params['cluster'] = cluster
    
    headers = {
        "Authorization": f"Bearer {api_key}",
        "X-USERNAME": current_user
    }
    
    # Sending the GET request to the Slurm REST API
    url =  f"{SLURMAPI_BASE}/jobs/"
    try:
        response = requests.get(url, headers=headers, params=params)
        #print(f'called requests.get. response={response.status_code}')
    except requests.RequestException as e:
        if logger:
            logger.error("Failed to call %s: %s", url, e)
        #print("Failed to call %s: %s", url, e)
        return None
    # Check if the request was successful
    if response.status_code == 200:
        data = response.json()
        # Try a couple of possible shapes
        if isinstance(data, dict):
            # slurmrestd-like: {"jobs": [...]}
            jobs = data.get("jobs")

            # or some SAW implementations may return directly a list
            if jobs is None and isinstance(data.get("job"), list):
                jobs = data["job"]
            elif jobs is None and isinstance(data.get("job"), dict):
                jobs = [data["job"]]
        elif isinstance(data, list):
            # pure list of jobs
            jobs = data
        else:
            jobs = None

        return jobs
    else:
        if logger:
            logger.error(
                "Failed to retrieve jobs. Status Code: %s, Message: %s",
                response.status_code,
                response.text[:500],
            )
        return None
 
if __name__ == "__main__":
    #ans = get_jobs()
    import pandas as pd
    from SharedConsts import QstatDataColumns, SRVER_USERNAME, JOB_CHANGE_COLS, JOB_ELAPSED_TIME, \
    JOB_RUNNING_TIME_LIMIT_IN_HOURS, JOB_NUMBER_COL, LONG_RUNNING_JOBS_NAME, QUEUE_JOBS_NAME, NEW_RUNNING_JOBS_NAME, \
    FINISHED_JOBS_NAME, JOB_STATUS_COL, WEIRD_BEHAVIOR_JOB_TO_CHECK, ERROR_JOBS_NAME, PATH2SAVE_PREVIOUS_DF, ACCOUNT_NAME, JOB_NAME_COL
    job_prefixes = ['KR','OP','PP']
    
    for prefix in job_prefixes: 
        results_df = pd.DataFrame(get_jobs(account=ACCOUNT_NAME, logger='<Logger main (DEBUG)>'))
        results_df = results_df[[JOB_NUMBER_COL, JOB_NAME_COL, 'job_state']]
        results_df = results_df[results_df[JOB_NAME_COL].str.startswith(prefix)]
        #results_df[['state', 'reason']] = results_df['job_state'].apply(pd.Series)
        results_df['state'] = results_df['job_state'].astype(str)
        results_df['reason'] = None
        #results_df['current_state'] = results_df['state'].apply(lambda x: ','.join(map(str, x)))
        results_df['current_state'] = results_df['job_state'].astype(str)
        print (str(results_df))