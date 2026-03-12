from pathlib import Path

# todo replace all paths
CODE_BASE_PATH = Path("/genomefltr/")
BASE_PATH_TO_KRAKEN_SCRIPT = Path("/genomefltr/bin/kraken/")
PATH_TO_DB_VALIDATOR_SCRIPT = Path("/genomefltr/KrakenHandlers/DbTestingScript.py")
PATH_TO_CUSTOM_GENOME_DOWNLOAD_SCRIPT = Path("/genomefltr/KrakenHandlers/GenomeDownload.py")
PATH_TO_VERIFY_INPUT_FILE_SCRIPT = Path("/genomefltr/KrakenHandlers/VerifyInputFile.py")
KRAKEN_SEARCH_SCRIPT_COMMAND = str(BASE_PATH_TO_KRAKEN_SCRIPT) + "/kraken2"
KRAKEN_CUSTOM_DB_SCRIPT_COMMAND = str(BASE_PATH_TO_KRAKEN_SCRIPT) + "/kraken2-build"
OUTPUT_MERGED_FASTA_FILE_NAME = f'merged.fasta'

# assuming the DB is in the same BASE folder as the kraken script
KRAKEN_DB_NAMES = {
                    'Bacteria': "RefSeq complete bacterial genomes",
                    'human': "GRCh38 human genome",
                    'fungi': "RefSeq complete fungal genomes",
                    'protozoa': "RefSeq complete protozoan genomes",
                    'UniVec': "NCBI-supplied database of vector, adapter, linker, and primer sequences that may be contaminating sequencing projects and/or assemblies",
                    'plasmid': "RefSeq plasmid nucleotide sequences",
                    'archaea': "RefSeq complete archaeal genomes",
                    'Viral': "RefSeq complete viral genomes",
                    'Kraken Standard': "complete genomes in RefSeq for the bacterial, archaeal, and viral domains, along with the human genome and a collection of known vectors (UniVec_Core)."
                  }
KRAKEN_RESULTS_FILE_PATH = BASE_PATH_TO_KRAKEN_SCRIPT / "Temp_Job_{job_unique_id}_results.txt"

# output processor Job variables
OUTPUT_PROCESSOR_JOB_QUEUE_NAME = 'pupko-pool'
NUBMER_OF_CPUS_OUTPUT_PROCESSOR_JOB = '1'
OUTPUT_PROCESSOR_JOB_PREFIX = 'OP'

# db updated Job variables
DP_UPDATOR_JOB_QUEUE_NAME = 'pupko-pool'
NUBMER_OF_CPUS_DB_UPDATOR = '10'
FOLDER_PATH_TO_DOWNLOAD_DB = '/genomefltr/kraken_DB/'
KRAKEN_UPDATER_JOB_PREFIX = 'UP'
FASTA_TESTING_PATH = "/genomefltr/example_process_results/reads.fasta"

# Kraken Search Job variables
KRAKEN_JOB_QUEUE_NAME = 'pupko-pool'
KRAKEN_JOB_ACCOUNT_NAME = 'pupko-users_v2'
KRAKEN_RESULTS_FILE_NAME = 'results.txt'
NUBMER_OF_CPUS_KRAKEN_SEARCH_JOB = '10'
KRAKEN_JOB_PREFIX = 'KR'
KRAKEN_CUSTOM_DB_JOB_PREFIX = 'CDB'
KRAKEN_CUSTOM_DB_NAME_PREFIX = 'CustomDB_'
CUSTOM_DB_TESTING_TMP_FILE = 'CustomDbTestingRes.txt'
KRAKEN_DB_MEM_REQS = {
    'Bacteria': '96G',
    'human': '80G',
    'fungi': '80G',
    'protozoa': '65G',
    'UniVec': '65G',
    'plasmid': '65G',
    'archaea': '65G',
    'Viral': '65G',
    'Kraken Standard': '96G',
    'MARTHA': '10G'
}
CREATE_FILE_FOR_POST_PROCESS_COMMAND = "cat {query_path} | /genomefltr/bin/seqkit grep -f {ids_list}  -o {ids_results}\n"

KRAKEN_JOB_TEMPLATE = '''#!/bin/bash\n\n

cd {kraken_base_folder}
PYTHONPATH=$(pwd)
pwd
sleep {sleep_interval}

VIRTUAL_ENV=/genomefltr/venv
PATH="/genomefltr/venv/bin:$PATH"

source {path_to_python_activate}
echo {path_to_python_activate}

which python

ls {path_to_python_activate}

python {path_to_input_validator_script} --folder2verify {path_to_folder} --file_name1_to_check {file_name_1} --file_name2_to_check {file_name_2}

{kraken_command} --db "{db_path}" "{query_path_string}" --output "{kraken_results_path}" --threads 20 --use-names --report "{report_file_path}" {additional_parameters}

#rm {query_path}
'''

OUTPUT_PROCESSOR_JOB_TEMPLATE = '''#!/bin/bash\n\n

PYTHONPATH=$(pwd)
pwd
sleep {sleep_interval}

source {path_to_python_activate}

ls {path_to_python_activate}

VIRTUAL_ENV=/genomefltr/venv
PATH="/genomefltr/venv/bin:$PATH"

which python

python {path_to_output_processor} --outputFilePath "{kraken_results_path}"

{create_files_for_post_process_commands}

#rm {query_path}
'''

# Kraken Custom Db Creation
KRAKEN_CUSTOM_DB_JOB_TEMPLATE = '''#!/bin/bash

VIRTUAL_ENV=/genomefltr/venv


source {path_to_python_activate}
export HOME={path_to_user_base_folder}
export XDG_CACHE_HOME={path_to_user_base_folder}

echo $HOME
echo $XDG_CACHE_HOME
echo ~/.cache

ls {path_to_python_activate}

cd {code_base_folder}

PYTHONPATH=$(pwd)

VIRTUAL_ENV=/genomefltr/venv
PATH="/genomefltr/venv/bin:$PATH"

which python

python {path_to_genome_download_script} --download_path {path_to_user_base_folder} --list_accession_number {list_of_accession_numbers} --list_taxids {list_of_taxaids}

DB_NAME="{kraken_base_folder}{custom_db_name}"

for i in 1,2,3
do

    rm -r -f $DB_NAME
    
    mkdir $DB_NAME
    
    mkdir $DB_NAME/taxonomy
    
    cp -R "{kraken_base_folder}Tax_Base/taxonomy/." $DB_NAME/taxonomy
    
    {kraken_db_command} --db $DB_NAME -add-to-library "{path_to_fasta_file}"
    
    {kraken_db_command} --db $DB_NAME --build --fast-build --threads {cpu_number}
    
    {kraken_db_command} --db $DB_NAME --clean --fast-build --threads {cpu_number}
    
    {kraken_run_command} --db {kraken_base_folder}/{custom_db_name} "{path_to_fasta_file}" --output "{testing_output_path}" --threads {cpu_number}
    
    python {path_to_validator_script} --TestingFastaPath "{testing_output_path}" 
    exit_code=$?
    if [[ $exit_code = 1 ]]; then
        echo "Broken";
        break;
    fi
      
done

'''

CREATE_NEW_DB_JOB_TEMPLATE = '''#!/bin/bash

VIRTUAL_ENV=/genomefltr/venv

source {path_to_python_activate}

cd {path_to_download_db}
PYTHONPATH=$(pwd)
DB_TYPE="{db_type}"
DB_NAME="{db_name}"

VIRTUAL_ENV=/genomefltr/venv
PATH="/genomefltr/venv/bin:$PATH"

which python

for i in 1,2,3
do
    if [[ $OK = 1 ]]; then
        break;
    fi
    mkdir $DB_NAME
    
    mkdir $DB_NAME/taxonomy
    
    cp -R "{kraken_base_folder}/Tax_Base/taxonomy/." {path_to_download_db}/$DB_NAME/taxonomy
    
    {kraken_base_folder}/kraken2-build --db $DB_NAME --download-library $DB_TYPE --threads 10
    
    {kraken_base_folder}/kraken2-build --db $DB_NAME --build --threads 10
    
    {kraken_base_folder}/kraken2-build --db $DB_NAME --clean --threads 10
    
    {kraken_base_folder}/kraken2 --db {path_to_download_db}/$DB_NAME "{testing_output_path}" --output {path_to_download_db}/$DB_NAME/kraken_out_Testing_DB.txt --threads 10
    python {path_to_validator_script} --TestingFastaPath "{path_to_download_db}/$DB_NAME/kraken_out_Testing_DB.txt" 
    
    exit_code=$?
    if [[ $exit_code = 1 ]]; then
        echo "Broken";
        rm -r $DB_NAME
        break;
    fi
    NEW_DB_NAME=$DB_NAME
    NEW_DB_NAME+="_ok"
    mv {path_to_download_db}/$DB_NAME {path_to_download_db}/$NEW_DB_NAME
    $OK=1
done
'''
