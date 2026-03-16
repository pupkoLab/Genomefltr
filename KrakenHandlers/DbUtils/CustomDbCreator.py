import sys
from KrakenConsts import KRAKEN_CUSTOM_DB_JOB_TEMPLATE, KRAKEN_CUSTOM_DB_SCRIPT_COMMAND,\
    BASE_PATH_TO_KRAKEN_SCRIPT, KRAKEN_CUSTOM_DB_JOB_PREFIX, NUBMER_OF_CPUS_KRAKEN_SEARCH_JOB, KRAKEN_JOB_QUEUE_NAME, \
    KRAKEN_CUSTOM_DB_NAME_PREFIX, KRAKEN_SEARCH_SCRIPT_COMMAND, CUSTOM_DB_TESTING_TMP_FILE, \
    PATH_TO_DB_VALIDATOR_SCRIPT, PATH_TO_CUSTOM_GENOME_DOWNLOAD_SCRIPT, CODE_BASE_PATH
from SharedConsts import PATH_2_PYTHON_ACTIVATE
import pathlib

from utils import logger
from subprocess import PIPE, run
from slurm_example import submit_job


class KrakenCustomDbCreator:

    @staticmethod
    def create_custom_db(path_to_fasta_file: str, list_of_taxaids: list, list_accession_number):
        """
        orchestrator method for creating a custom db from a fasta file.
        :param path_to_fasta_file: path to the fasta file
        assumes fasta file is STRICTLY in the format:
        >SEQ_NAME NCBI_ACCESSION_NUMBER
        ACTUAL_SEQ
        may contain many seqs in this format
        :param list_of_taxaids: list of NCBI taxaids numbers to download as the DB
        :param list_accession_number: list of lists of specific accession numbers to download. Each list refer to a different speices: [[acc1, acc2], [], [acc3]]. First list for the first species and so one (refering to same order as list_of_taxaids)
        :return: the PBS id number or None if unsuccessful
        """
        user_unique_id = str(pathlib.Path(path_to_fasta_file).parent.stem)
        path_to_fasta_file = pathlib.Path(path_to_fasta_file)
        custom_db_name = KRAKEN_CUSTOM_DB_NAME_PREFIX + user_unique_id
        if (path_to_fasta_file.parent / custom_db_name).is_dir():
            return None  # the custom db already exists
        testing_output_path = path_to_fasta_file.parent / CUSTOM_DB_TESTING_TMP_FILE

        run_parameters = KrakenCustomDbCreator._parse_db_job_text(custom_db_name, path_to_fasta_file,
                                                                    testing_output_path, list_of_taxaids, list_accession_number)

        run_parameters['logger'] = logger

        # run the job
        #with open(temp_script_path, 'w+') as fp:
        #    fp.write(temp_script_text)
        #logger.info(f'submitting job, temp_script_path = {temp_script_path}:')
        logger.debug(f'{run_parameters}')
        #terminal_cmd = f'/opt/pbs/bin/qsub {str(temp_script_path)}'
        #job_run_output = subprocess.run(terminal_cmd, stdout=PIPE, stderr=PIPE, shell=True)
        # os.remove(temp_script_path)
        
        return submit_job(**run_parameters)
        
    @staticmethod
    def _parse_db_job_text(custom_db_name: str, path_to_fasta_file: pathlib.Path, testing_output_path: pathlib.Path,
                           list_of_taxaids: list, list_accession_number: list):
        """
        parses the .sh file to be submitted for the custom creation job
        :param custom_db_name: name of the custom db to be created
        :param path_to_fasta_file: path to the fasta file
        :param testing_output_path: path to testing results path
        :param list_of_taxaids: list of NCBI taxaids numbers to download as the DB
        :param list_accession_number: list of lists of specific accession numbers to download. Each list refer to a different speices: [[acc1, acc2], [], [acc3]]. First list for the first species and so one (refering to same order as list_of_taxaids)
        :return:
        """
        queue_name = KRAKEN_JOB_QUEUE_NAME
        cpu_number = NUBMER_OF_CPUS_KRAKEN_SEARCH_JOB
        job_name = f'{KRAKEN_CUSTOM_DB_JOB_PREFIX}_{custom_db_name}'
        job_logs_path = str(pathlib.Path(path_to_fasta_file).parent) + '/'
        kraken_base_folder = str(BASE_PATH_TO_KRAKEN_SCRIPT) + '/'
        custom_db_name = custom_db_name
        list_of_taxaids_str = str(list_of_taxaids).strip('[]')
        list_accession_number = [','.join(x) for x in list_accession_number]
        list_of_accession_numbers_str = '@@'.join(list_accession_number) # for the above example the result should be: acc1,acc2@@@@acc3
        custom_db_sh_text = KRAKEN_CUSTOM_DB_JOB_TEMPLATE.format(cpu_number=cpu_number,
                                                                 kraken_base_folder=kraken_base_folder,
                                                                 custom_db_name=custom_db_name,
                                                                 code_base_folder=CODE_BASE_PATH,
                                                                 kraken_db_command=KRAKEN_CUSTOM_DB_SCRIPT_COMMAND,
                                                                 kraken_run_command=KRAKEN_SEARCH_SCRIPT_COMMAND,
                                                                 testing_output_path=str(testing_output_path),
                                                                 path_to_fasta_file=path_to_fasta_file,
                                                                 path_to_validator_script=PATH_TO_DB_VALIDATOR_SCRIPT,
                                                                 path_to_user_base_folder=str(pathlib.Path(path_to_fasta_file).parent),
                                                                 list_of_taxaids=list_of_taxaids_str,
                                                                 list_of_accession_numbers=list_of_accession_numbers_str,
                                                                 path_to_genome_download_script=PATH_TO_CUSTOM_GENOME_DOWNLOAD_SCRIPT,
                                                                 path_to_python_activate=PATH_2_PYTHON_ACTIVATE)

        return {"queue": queue_name, 
                "num_cpus": cpu_number, 
                "job_name": job_name,
                "logs_path": job_logs_path,
                "script_commands": custom_db_sh_text,
                "memory": "80G"}
