import os
import pathlib
import subprocess
from submit_slurm import submit_job
from subprocess import PIPE
from SharedConsts import RESULTS_FOR_OUTPUT_CLASSIFIED_RAW_FILE_NAME, RESULTS_FOR_OUTPUT_UNCLASSIFIED_RAW_FILE_NAME, \
    INPUT_UNCLASSIFIED_FILE_NAME, INPUT_CLASSIFIED_FILE_NAME, FINAL_OUTPUT_FILE_NAME, \
    INTERVAL_BETWEEN_LISTENER_SAMPLES, POSTPROCESS_JOB_PREFIX, POST_PROCESS_COMMAND_TEMPLATE, \
    POSTPROCESS_JOB_QUEUE_NAME, POSTPROCESS_JOB_ACCOUNT_NAME, NUBMER_OF_CPUS_POSTPROCESS_JOB, FILTER_ORIGINAL_CLASSIFIED_RESULTS, \
    INPUT_CLASSIFIED_FILE_NAME_PAIRED, INPUT_UNCLASSIFIED_FILE_NAME_PAIRED, COMBINE_RESULTS_PAIRED, \
    FINAL_OUTPUT_FILE_NAME_PAIRED, GZIP_LINE_PAIRED, GZIP_LINE_ONE_FILE, FINAL_OUTPUT_ZIPPED_BOTH_FILES, \
    NEW_CONTAMINATION_PAIRED_CREATION_LINE, NEW_CONTAMINATION_FASTA, NEW_CONTAMINATION_FASTA_PAIRED, \
    GZIP_LINE_ONE_FILE_CONTAMINATED, GZIP_LINE_PAIRED_NEW_CONTAMINATED, FINAL_OUTPUT_ZIPPED_BOTH_FILES_NEW_CONTAMINATED, \
    PATH_TO_TEMP_AWK_PARAM_FILE, CHECK_IF_LINES_SUIT_PAIRED_FILE, PATH_2_PYTHON_ACTIVATE

from utils import logger


def run_post_process(root_folder, classification_threshold, species_to_filter_on, is_paired=False):
    """
    this function runs the post process filtering of classified to unclassified results
    :param root_folder: path to the client root folder
    :param classification_threshold: threshold to determine "classified" results as "unclassified" results
    :param species_to_filter_on: list of species to KEEP classified
    :return: the PBS job id
    """

    path_to_classified_results = os.path.join(root_folder, RESULTS_FOR_OUTPUT_CLASSIFIED_RAW_FILE_NAME)
    path_to_unclassified_results = os.path.join(root_folder, RESULTS_FOR_OUTPUT_UNCLASSIFIED_RAW_FILE_NAME)
    path_to_temp_awk_param_file = os.path.join(root_folder, PATH_TO_TEMP_AWK_PARAM_FILE)
    path_to_original_unclassified_data = os.path.join(root_folder, INPUT_UNCLASSIFIED_FILE_NAME)
    path_to_original_classified_data = os.path.join(root_folder, INPUT_CLASSIFIED_FILE_NAME)
    fasta_output_file, ext = os.path.splitext(str(FINAL_OUTPUT_FILE_NAME))
    path_to_final_result_file = os.path.join(root_folder, fasta_output_file)
    species_to_filter_on_string = str(species_to_filter_on).strip('[]').replace('\'', "").replace(', ', ',')
    job_unique_id = str(pathlib.Path(root_folder).stem)
    job_name = f'{POSTPROCESS_JOB_PREFIX}_{job_unique_id}'
    job_logs_path = str(root_folder) + '/'
    path_to_new_contamination_seqs = os.path.join(root_folder, NEW_CONTAMINATION_FASTA)
    path_to_temp_new_contamination_seqs = os.path.join(root_folder, 'TEMP_NEW_CONTAMINATION_FASTA.fasta')
    path_to_temp_file = os.path.join(str(root_folder), 'Temp.txt')
    path_to_temp_unclassified = os.path.join(str(root_folder), 'Temp_new_unclassified_seqs.fasta')
    path_to_temp_last_digits_file = os.path.join(str(root_folder), 'last_digits_file.txt')
    path_to_temp_first_read_file = os.path.join(str(root_folder), 'Temp1.txt')
    path_to_temp_second_read_file = os.path.join(str(root_folder), 'Temp2.txt')

    if is_paired:
        path_to_new_contamination_seqs_paired = os.path.join(root_folder, NEW_CONTAMINATION_FASTA_PAIRED)
        filter_original_classified_results_line = FILTER_ORIGINAL_CLASSIFIED_RESULTS
        create_new_contamination_paired_line = NEW_CONTAMINATION_PAIRED_CREATION_LINE
        path_to_original_unclassified_data_paired = os.path.join(root_folder, INPUT_UNCLASSIFIED_FILE_NAME_PAIRED)
        path_to_original_classified_data_paired = os.path.join(root_folder, INPUT_CLASSIFIED_FILE_NAME_PAIRED)
        combine_results_paired_line = COMBINE_RESULTS_PAIRED
        path_to_temp_unclassified_file_paired = os.path.join(str(root_folder), 'Temp_new_unclassified_seqs_paired.fasta')
        fasta_output_file_paired, ext = os.path.splitext(str(FINAL_OUTPUT_FILE_NAME_PAIRED))
        path_to_final_result_file_paired = os.path.join(root_folder, fasta_output_file_paired)
        gzip_line = GZIP_LINE_PAIRED.format(path_to_process_folder=root_folder,
                                            tar_result_file_name=str(FINAL_OUTPUT_ZIPPED_BOTH_FILES),
                                            output_one_file_name=str(fasta_output_file),
                                            output_paired_file_name=str(fasta_output_file_paired))
        gzip_line_for_contaminated = GZIP_LINE_PAIRED_NEW_CONTAMINATED.format(
            path_to_process_folder=root_folder,
            tar_result_file_name=str(FINAL_OUTPUT_ZIPPED_BOTH_FILES_NEW_CONTAMINATED),
            output_one_file_name=str(NEW_CONTAMINATION_FASTA),
            output_paired_file_name=str(NEW_CONTAMINATION_FASTA_PAIRED))
        check_if_lines_suit_paired_file=CHECK_IF_LINES_SUIT_PAIRED_FILE

    else:
        path_to_new_contamination_seqs_paired = ''
        filter_original_classified_results_line = ''
        create_new_contamination_paired_line = ''
        path_to_original_unclassified_data_paired = ''
        path_to_original_classified_data_paired = ''
        combine_results_paired_line = ''
        path_to_temp_unclassified_file_paired = ''
        path_to_final_result_file_paired = ''
        check_if_lines_suit_paired_file = ''
        gzip_line = GZIP_LINE_ONE_FILE
        gzip_line_for_contaminated = GZIP_LINE_ONE_FILE_CONTAMINATED

    command_to_run = POST_PROCESS_COMMAND_TEMPLATE.format(path_to_python_activate=PATH_2_PYTHON_ACTIVATE,
                                                          path_to_classified_results=path_to_classified_results,
                                                          path_to_final_result_file=path_to_final_result_file,
                                                          classification_threshold=classification_threshold,
                                                          species_to_filter_on=species_to_filter_on_string,
                                                          path_to_original_unclassified_data=path_to_original_unclassified_data,
                                                          path_to_original_classified_data=path_to_original_classified_data,
                                                          path_to_temp_file=path_to_temp_file,
                                                          path_to_temp_unclassified_file=path_to_temp_unclassified,
                                                          sleep_interval=INTERVAL_BETWEEN_LISTENER_SAMPLES,
                                                          path_to_original_classified_data_paired=path_to_original_classified_data_paired,
                                                          path_to_original_unclassified_data_paired=path_to_original_unclassified_data_paired,
                                                          Temp_new_unclassified_seqs_paired=path_to_temp_unclassified_file_paired,
                                                          filter_original_classified_results_line=filter_original_classified_results_line,
                                                          combine_results_paired_line=combine_results_paired_line,
                                                          gzip_line=gzip_line,
                                                          path_to_final_result_file_paired=path_to_final_result_file_paired,
                                                          path_to_temp_unclassified_file_paired=path_to_temp_unclassified_file_paired,
                                                          create_new_contamination_paired_line=create_new_contamination_paired_line,
                                                          path_to_new_contamination_seqs=path_to_new_contamination_seqs,
                                                          path_to_temp_new_contamination_seqs=path_to_temp_new_contamination_seqs,
                                                          path_to_new_contamination_seqs_paired=path_to_new_contamination_seqs_paired,
                                                          gzip_line_new_contaminated=gzip_line_for_contaminated,
                                                          path_to_temp_awk_param_file=path_to_temp_awk_param_file,
                                                          path_to_temp_last_digits_file=path_to_temp_last_digits_file,
                                                          path_to_temp_first_read_file=path_to_temp_first_read_file,
                                                          path_to_temp_second_read_file=path_to_temp_second_read_file,
                                                          check_if_lines_suit_paired_file=check_if_lines_suit_paired_file
                                                          )

    # run post process on PBS
    #temp_script_path = os.path.join(str(root_folder), f'TempPostProcessFor.sh')

    #with open(temp_script_path, 'w+') as fp:
    #    fp.write(command_to_run)
    #logger.info(f'submitting job, temp_script_path = {temp_script_path}:')
    #logger.debug(f'{command_to_run}')
    #terminal_cmd = f'/opt/pbs/bin/qsub {str(temp_script_path)}'
    #job_run_output = subprocess.run(terminal_cmd, stdout=PIPE, stderr=PIPE, shell=True)
    # os.remove(temp_script_path)
    run_parameters = {"queue": POSTPROCESS_JOB_QUEUE_NAME, 
                "account_name": POSTPROCESS_JOB_ACCOUNT_NAME,
                "num_cpus": NUBMER_OF_CPUS_POSTPROCESS_JOB, 
                "job_name": job_name,
                "logs_path": job_logs_path,
                "script_commands": command_to_run,
                "memory": "60G"}
    run_parameters['logger'] = logger
    logger.debug(f'{run_parameters}')
    return submit_job(**run_parameters)

