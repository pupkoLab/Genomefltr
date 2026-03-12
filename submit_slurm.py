#!/usr/bin/python

import os
import sys
import requests
from os.path import normpath, basename
import logging
import re

current_user = "genomefltr"
api_key = "VgpwXObaQz0xIgAAvzvn2jIQ5Jzo2ElkI8ENlbmwXAc"

# Base URL for authentication and token generation
base_url = 'https://saw.tau.ac.il'

def submit_job(script_commands, job_name, logs_path, num_cpus, queue, account_name, memory, logger):

    # job submission url
    job_url = f'{base_url}/slurmapi/job/submit/'
    # Auth Headers
    headers = {
        "Authorization": f"Bearer {api_key}",
        'X-USERNAME': current_user,
        }

    #jobID = basename(normpath(wd))
    #jobName = f'genomfltr_{jobID}'
    
    memory_in_M = 0
    if isinstance(memory, str):
        if "G" == memory[-1]:
            memory_in_M = int(re.findall(r'\d+', memory)[0]) * 1000
        else:
            memory_in_M = int(re.findall(r'\d+', memory)[0])
    else:
        memory_in_M = memory
    print (f"script_commands = {script_commands}")
    
    payload = {
        "script": script_commands,
        "partition": queue,
        "tasks": 1,
        "name": job_name,
        "account": account_name,
        "nodes": "1",
        "qos": "owner",
                     # how much CPU you need
        "cpus_per_task": int(num_cpus),
                     # How much Memory you need per node, in MB
        #"memory_per_node": 6144,
        "memory_per_node": int(memory_in_M),
        "time_limit": {
            "number": 24*60*7,
            "set": True,
            "infinite": False
        },
        #"standard_output": wd + "/output.txt",
        #"standard_error": wd + "/error.txt",
        "standard_output": os.path.join(logs_path, 'output_%j.txt'),
        "standard_error": os.path.join(logs_path, 'errors_%j.txt'),
        # List of nodes where the job must be allocated (uncomment the below 3 lines, and specify node name)
        "current_working_directory": "/tmp/",
        "environment": [
            "PATH=/powerapps/share/bin:/usr/local/bin:/usr/bin:/usr/local/sbin:/usr/sbin:/usr/local.cc/bin:/mathematica/vers/11.2:/powerapps/share/rocky8/gcc-13.1.0/bin:",
            "MODULEPATH=/powerapps/share/Modules/Centos7/modulefiles:/powerapps/share/Modules/Rocky8/modulefiles:/powerapps/share/Modules/Rocky9/modulefiles:/etc/modulefiles:/usr/share/modulefiles:/usr/share/modulefiles/Linux:/usr/share/modulefiles/Core:/usr/share/lmod/lmod/modulefiles/Core"
        ]
    }

    import json
    print("Final payload:")
    print(json.dumps(payload, indent=2))
    jobs_request = requests.post(job_url, headers=headers, json=payload)
    print ('called requests.post')
    jobs_result = jobs_request.json()
    
    return str(jobs_result)

if __name__ == "__main__":
    job_name = sys.argv[1]
    
    #parameters = {'queue': 'pupko-pool', 'account_name':'pupko-users_v2', 'num_cpus': '10', 'job_name': job_name, 'logs_path': f'/genomefltr/user_results/{job_name}/', 'script_commands': f'#!/bin/bash\n\n\n\ncd /genomefltr\nPYTHONPATH=$(pwd)\npwd\nsleep 180\n\nVIRTUAL_ENV=/genomefltr/venv\n\nsource /genomefltr/venv/bin/activate\n\nls /genomefltr/venv/bin/activate\n\npython /genomefltr/KrakenHandlers/VerifyInputFile.py --folder2verify /genomefltr/user_results/{job_name} --file_name1_to_check reads.fasta --file_name2_to_check reads2.fasta\n\n/genomefltr/bin/kraken/kraken2 --db "/genomefltr/bin/kraken/BasicDB" "/genomefltr/user_results/{job_name}/reads.fasta" --output "/genomefltr/user_results/{job_name}/results.txt" --threads 20 --use-names --report "/genomefltr/user_results/{job_name}/summary_results.txt" \n\n#rm /genomefltr/user_results/{job_name}/reads.fasta\n', 'memory': '96G', 'logger': '<Logger main (DEBUG)>'}
    
    #from KrakenHandlers.SearchEngine import SearchEngine
    #ans = SearchEngine.output_processor(f'/genomefltr/user_results/{job_name}')
    
    #parameters = {'queue': 'pupko-pool', 'account_name':'pupko-users_v2', 'num_cpus': '10', 'job_name': job_name, 'logs_path': f'/genomefltr/user_results/{job_name}/', 'script_commands': f'#!/bin/bash\n\n\n\ncd /genomefltr\nPYTHONPATH=$(pwd)\npwd\nsleep 180\n\nVIRTUAL_ENV=/genomefltr/venv\n\nsource /genomefltr/venv/bin/activate\n\nls /genomefltr/venv/bin/activate\n\npython /genomefltr/KrakenHandlers/VerifyInputFile.py --folder2verify /genomefltr/user_results/{job_name} --file_name1_to_check reads.fasta --file_name2_to_check reads2.fasta\n\n/genomefltr/bin/kraken/kraken2 --db "/genomefltr/bin/kraken/BasicDB" "/genomefltr/user_results/{job_name}/reads.fasta" --output "/genomefltr/user_results/{job_name}/results.txt" --threads 20 --use-names --report "/genomefltr/user_results/{job_name}/summary_results.txt" \n\n#rm /genomefltr/user_results/{job_name}/reads.fasta\n', 'memory': '96G', 'logger': '<Logger main (DEBUG)>'}
    
    #print(f'{str(parameters)}\n')
    
    parameters = {'queue': 'pupko-pool', 'account_name':'pupko-users_v2', 'num_cpus': '10', 'job_name': job_name, 'logs_path': f'/genomefltr/user_results/{job_name}/', 'script_commands': "#!/bin/bash\npython --version\n", 'memory': '96G', 'logger': '<Logger main (DEBUG)>'}
    print('submit_job')
    ans = submit_job (**parameters)
    print (ans, end='\n')