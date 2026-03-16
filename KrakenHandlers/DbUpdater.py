import pathlib
import subprocess
from subprocess import PIPE
import os
from utils import logger
from KrakenConsts import BASE_PATH_TO_KRAKEN_SCRIPT, KRAKEN_SEARCH_SCRIPT_COMMAND, KRAKEN_DB_NAMES, \
    KRAKEN_JOB_TEMPLATE, DP_UPDATOR_JOB_QUEUE_NAME, NUBMER_OF_CPUS_DB_UPDATOR, CODE_BASE_PATH, FOLDER_PATH_TO_DOWNLOAD_DB, \
    KRAKEN_CUSTOM_DB_NAME_PREFIX, KRAKEN_DB_MEM_REQS, CREATE_NEW_DB_JOB_TEMPLATE, KRAKEN_UPDATER_JOB_PREFIX, FASTA_TESTING_PATH, \
    PATH_TO_DB_VALIDATOR_SCRIPT
from SharedConsts import PATH_TO_OUTPUT_PROCESSOR_SCRIPT, RESULTS_SUMMARY_FILE_NAME, INPUT_CLASSIFIED_FILE_NAME, \
    INPUT_UNCLASSIFIED_FILE_NAME, TEMP_CLASSIFIED_IDS, TEMP_UNCLASSIFIED_IDS, \
    INPUT_CLASSIFIED_FILE_NAME_PAIRED, INPUT_UNCLASSIFIED_FILE_NAME_PAIRED, PATH_2_PYTHON_ACTIVATE
import datetime
import glob
import shutil
from slurm_example import submit_job

class DbUpdater:
    """
    a class holding all code related to running the kraken2 search - assumes all inputs are valid
    """
    @staticmethod
    def check_if_download_is_needed(db_type: str):
        """
        Check if need to download a new db. Also deletes old db folders
        :param db_type: must be one of these strings: 'Bacteria', 'human', 'fungi', 'protozoa', 'UniVec', 'plasmid',
        'archaea', 'Viral','BasicDB'
        :return: boolean, if need to download a new db
        """
        base_folder = os.path.join(FOLDER_PATH_TO_DOWNLOAD_DB, db_type)
        if not os.path.isdir(base_folder):
            # we need to download the db
            return True
        list_of_folders = glob.glob(f"{base_folder}/*")
        # it is possible that some kraken job still use not the latest dir
        # thus we keep them for 10 more days before deleting
        # we calculate the time from DAYS_TO_UPDATE_DB + DAYS_TO_KEEP_NOT_LATEST_DIR in order to delete folder
        DAYS_TO_KEEP_NOT_LATEST_DIR = 10
        # a db updater job will start 30 days after the latest job
        DAYS_TO_UPDATE_DB = 30
        # list of tuples with the folder_date and the path
        list_of_ok_dir = []
        for folder in list_of_folders:
            if not os.path.isdir(folder):
                # not a dir
                continue
            folder_name = os.path.basename(folder)
            if '_ok' in folder_name:
                folder_date = datetime.datetime.strptime(folder_name.replace('_ok', ''), '%Y_%m_%d_%H')
                list_of_ok_dir.append((folder_date, folder))
        
        if len(list_of_ok_dir) == 0:
            # we need to download the db
            return True
        
        current_time = datetime.datetime.today()
        # get the folder with the latest date
        latest_dir = max(list_of_ok_dir, key= lambda x: x[0])
        list_of_ok_dir.remove(latest_dir)
        # all of the folders in the list should be deleted after certain time
        for folder in list_of_ok_dir:
            folder_path = folder[1]
            folder_date = folder[0]
            date_differences = current_time - folder_date
            differences_days = date_differences.days
            if differences_days > DAYS_TO_KEEP_NOT_LATEST_DIR + DAYS_TO_UPDATE_DB:
                shutil.rmtree(folder_path)
            
        latest_dir_date = latest_dir[0]
        date_differences = current_time - latest_dir_date
        differences_days = date_differences.days
        
        if differences_days > DAYS_TO_UPDATE_DB:
            # we need to run the db updator
            return True
        # we don't need to run the db updator
        return False

    @staticmethod
    def update_db(db_type: str):
        """
        Creates a new kraken of db type
        :param db_type: must be one of these strings: 'Bacteria', 'human', 'fungi', 'protozoa', 'UniVec', 'plasmid',
        'archaea', 'Viral','BasicDB'
        :return: created job id
        """
        current_date = str(datetime.datetime.today().strftime('%Y_%m_%d_%H'))
        db_name = f'{current_date}'
        target_dir = os.path.join(FOLDER_PATH_TO_DOWNLOAD_DB, db_type)
        testing_output_path = FASTA_TESTING_PATH
        os.makedirs(target_dir, exist_ok=True)
        # create the job
        run_parameters = DbUpdater._create_db_update_job_text(db_type=db_type, db_name=db_name, job_id=current_date,
                                                                testing_output_path=testing_output_path, target_dir=target_dir)
        # run the job
        run_parameters['logger'] = logger
        # run the job
        #with open(temp_script_path, 'w+') as fp:
        #    fp.write(temp_script_text)
        #logger.info(f'submitting job, temp_script_path = {temp_script_path}:')
        logger.debug(f'{run_parameters}')
        #terminal_cmd = f'slurm_example.py {str(temp_script_path)}'
        #job_run_output = subprocess.run(terminal_cmd, stdout=PIPE, stderr=PIPE, shell=True)
        # os.remove(temp_script_path)
        
        return submit_job(**run_parameters)

    @staticmethod
    def _create_db_update_job_text(db_name: str, db_type: str, job_id: str, testing_output_path:str, target_dir:str):
        """
        this function creates the text for the .sh file that will run the job - assumes everything is valid
        :param db_name: unique name of the db
        :param db_type: type of the db
        :param job_id: the jobs unique id (used to identify everything related to this run)
        :param testing_output_path: path to testing fasta
        :param target_dir: path to download the DB
        :return: the text for the .sh file
        """

        job_name = f'{KRAKEN_UPDATER_JOB_PREFIX}_{job_id}'
        logs_folder = os.path.join(target_dir, 'logs')
        os.makedirs(logs_folder, exist_ok=True)
        script_commands = CREATE_NEW_DB_JOB_TEMPLATE.format(kraken_base_folder=BASE_PATH_TO_KRAKEN_SCRIPT, db_type=db_type,
                                                    db_name=db_name, path_to_download_db=target_dir,
                                                    path_to_validator_script=PATH_TO_DB_VALIDATOR_SCRIPT,
                                                    testing_output_path=testing_output_path,
                                                    path_to_python_activate=PATH_2_PYTHON_ACTIVATE)
        return {"queue": DP_UPDATOR_JOB_QUEUE_NAME, 
                "num_cpus": NUBMER_OF_CPUS_DB_UPDATOR, 
                "job_name": job_name,
                "logs_path": logs_folder,
                "script_commands": script_commands,
                "memory": "60"}

    @staticmethod
    def run_db_update():
        """
        this function runs every fews days and create the DB updator jobs
        :return: 
        """
        db_candidates = list(KRAKEN_DB_NAMES.keys())
        for db_candidate in db_candidates:
            if db_candidate == 'Bacteria':
                db_candidate = 'bacteria'
            if db_candidate == 'Viral':
                db_candidate = 'viral'
            is_download_needed = DbUpdater.check_if_download_is_needed(db_candidate)
            logger.info(f'db_candidate = {db_candidate}, is_download_needed = {is_download_needed}')
            if is_download_needed:
                logger.info(f'starting download')
                DbUpdater.update_db(db_candidate)

if __name__ == '__main__':
    try:
        DbUpdater.run_db_update()
    except Exception as e:
        logger.exception("Exception :")
