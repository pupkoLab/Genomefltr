#!/usr/bin/python3
import requests
import os
import re

# Base URL for authentication and token generation
base_url_auth = 'https://saw.tau.ac.il/slurmapi/jobs/submit/'
generate_token_url = f"{base_url_auth}/slurmapi/generate-token/"
# Base URL for job submission
base_url = f"{base_url_auth}/slurmrestd"
# Job submission URL
job_url = f'{base_url}/slurm/v0.0.40/job/submit'

# User credentials
current_user = "genomefltr"
api_key = "wscQW59c67zSt0n31A1vxFw8OC4QKsqHN5dX4ucz5SY6Q8U0QcCBtcWTuZcM7CIx9_E"


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


def submit_job(script_commands, job_name, logs_path, num_cpus, queue, memory, logger):
    logger.info(f'in submit_job, for {job_name}')
    # Authorization headers with the obtained token
    headers = {
        #'X-SLURM-USER-NAME': current_user,
        #'X-SLURM-USER-TOKEN': get_api_token(current_user, api_key)
        "Authorization": f"Bearer {api_key}",
        "X-USERNAME": current_user
    }
    
    memory_in_M = 0
    if isinstance(memory, str):
        if "G" == memory[-1]:
            memory_in_M = int(re.findall(r'\d+', memory)[0]) * 1000
        else:
            memory_in_M = int(re.findall(r'\d+', memory)[0])
    else:
        memory_in_M = memory
    
    logger.info(f'submitting with memory_in_M = {memory_in_M}, memory = {memory}')
    # Job submission request
    jobs_request = requests.post(
        job_url,
        headers=headers,
        json={
            # Example job script
            "script": script_commands,
            "job": {
                "partition": queue,
                "tasks": 1,
                "name": job_name,
                # we should by the time limit in minutes, which equal 24 hours times 7 days times 60.
                "time_limit": 24 * 60 * 7,
                #"account": "< account_name >",
                "nodes": "1",
                #"nodelist": "compute-0-299", # should be moved
                #"required_nodes": ['compute-0-299'],
                "cpus_per_task": int(num_cpus),
                "memory_per_node": {
                    "number": memory_in_M,
                    "set": True,
                    "infinite": False
                },
                # Full path to your error/output file.
                "standard_output": os.path.join(logs_path, 'output_%j.txt'),
                "standard_error": os.path.join(logs_path, 'errors_%j.txt'),
                "current_working_directory": "/tmp/",
                # Environment modules (module load) should not be used directly under the script parameter. Instead, set all necessary environment variables under the environment parameter.
                "environment": [
                    "PATH=/powerapps/share/rocky8/gcc-13.1.0/bin:/genomefltr/venv/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/root/bin:/a/home/cc/lifesci/genomefltr/.pyenv/shims:/a/home/cc/lifesci/genomefltr/.pyenv/bin",
                    "LD_LIBRARY_PATH=/lib/:/lib64/:/usr/local/lib:/powerapps/share/rocky8/gcc-13.1.0/lib64:/powerapps/share/rocky8/gcc-13.1.0/lib:/powerapps/src/rocky8/gcc-13.1.0/isl/lib:/powerapps/src/rocky8/gcc-13.1.0/mpfr-4.2.0/lib:/powerapps/src/rocky8/gcc-13.1.0/gmp-6.2.1/lib"
                ],
            },
        }
    )
    

    # Processing the job submission result
    logger.info(jobs_request)
    jobs_result = jobs_request.json()
    logger.info(f'jobs_result {jobs_result}')
    if 'result' in jobs_result:
        jobs_request = jobs_result['result']
        logger.info(jobs_result)
        for key, value in jobs_result.items():
            if 'job_id' in key:
                logger.info(f'submitted job_id {value}')
                return value
    else:
        logger.error(f'failed to submit job {job_name} return from job = {jobs_result}')

#import logging
#import sys
#
#logger = logging.getLogger(__name__)
#job_name = sys.argv[1]
##submit_job("#!/bin/bash\n\nsource /genomefltr/venv/bin/activate\nsleep 5\npython --version", 'test', "/genomefltr/logs/", "1", "pupko-pool", "1", logger)
#parameters = {'queue': 'pupko-pool', 'num_cpus': '10', 'job_name': job_name, 'logs_path': f'/genomefltr/user_results/{job_name}/', 'script_commands': "#!/bin/bash\npython --version\n", 'memory': '96G', 'logger': logger}
#print('submit_job')
#ans = submit_job (**parameters)
#print (ans, end='\n')