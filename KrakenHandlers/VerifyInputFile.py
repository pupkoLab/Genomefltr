import argparse
import os
import gzip
import shutil
from Bio import SeqIO, Entrez

def find_file_path(file2check):
    """finds if the file exists (zip or unzipped)

    Parameters
    ----------
    file2check : str
        The path of the file to check

    Returns
    -------
    path_of_file: str
       Returns the path of the file if exists, else False
    """
    if os.path.isfile(file2check):
        return file2check
    file2check += '.gz' #maybe it is zipped
    if os.path.isfile(file2check):
        return file2check
    return False

def delete_folder(folder2remove):
    """Deletes folder.
    Used when the file isn't valid

    Parameters
    ----------
    folder2remove : str
        folder to remove

    Returns
    -------
    """
    print(f'delete_folder folder2remove = {folder2remove}')
    shutil.rmtree(folder2remove)

def is_fasta(file_path):
    """the function verify if the file is Fasta (not only the name but also the content)
    
    Parameters
    ----------
    file_path: str
        the path to the file

    Returns
    -------
    is_fasta: bool
        True if fasta file, else False
    """
    with open(file_path, "r") as handle:
        fasta = SeqIO.parse(handle, "fasta")
        try:
            return any(fasta)
        except Exception as e:
            return False

def is_fastq(file_path):
    """the function verify if the file is Fastaq (not only the name but also the content)
    
    Parameters
    ----------
    file_path: str
        the path to the file

    Returns
    -------
    is_fastaq: bool
        True if fastaq file, else False
    """
    with open(file_path, "r") as handle:
        fastq = SeqIO.parse(handle, "fastq")

        try:
            return any(fastq)
        except Exception as e:
            return False

def unzip_file(file_path):
    """this function unzip a gz file
    
    Parameters
    ----------
    file_path: str
        the path to the file

    Returns
    -------
    unzipped_file_path: str
        The path to the unzipped file
    """
    with gzip.open(file_path, 'rb') as f_in:
        unzipped_file_path = '.'.join(file_path.split('.')[:-1])
        with open(unzipped_file_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    return unzipped_file_path

def validate_input_file(file2check):
    """this function validates the input file.
    It tests if the file is fasta or fastaq (if it's gz then it first unzip and then tests if fasta or fastaq)
    
    Parameters
    ----------
    file2check: str
        the path to the file

    Returns
    -------
    is_valid: bool
        True if the file is valid, else False
    """
    if file2check.endswith('.gz'): #unzip file
        file2check = unzip_file(file2check)
    
    if is_fasta(file2check):
        return True
    elif is_fastq(file2check):
        return True
    return False

if __name__ == "__main__":
    """validate input file by testing the file itself.
    Will ungzip file if needed

    Parameters
    ----------
    process_id : str
        The ID of the process

    Returns
    -------
    is_valid: bool
        True if the file is valid, else False.
    """
    CLI=argparse.ArgumentParser()
    CLI.add_argument(
      "--folder2verify",  # name on the CLI - drop the `--` for positional/required parameters
      nargs="+",  # 0 or more values expected => creates a list
      type=str,
    )
    CLI.add_argument(
      "--file_name1_to_check",  # name on the CLI - drop the `--` for positional/required parameters
      nargs="+",  # 0 or more values expected => creates a list
      type=str,
    )
    CLI.add_argument(
      "--file_name2_to_check",  # name on the CLI - drop the `--` for positional/required parameters
      nargs="+",  # 0 or more values expected => creates a list
      type=str,
    )
    
    args = CLI.parse_args()
    
    print("folder2verify: ", args.folder2verify[0])
    print("file_name1_to_check: ", args.file_name1_to_check[0])
    print("file_name2_to_check: ", args.file_name2_to_check[0])
    
    input_file_name = args.file_name1_to_check[0]
    input_file_name2 = args.file_name2_to_check[0]
    parent_folder = args.folder2verify[0]
    if not os.path.isdir(parent_folder):
        exit(f'process_id = {process_id} doen\'t have a dir')
    file2check = os.path.join(parent_folder, input_file_name)
    file2check = find_file_path(file2check)
    file2check2 = os.path.join(parent_folder, input_file_name2) # for paired reads
    file2check2 = find_file_path(file2check2)
    # test file in the input_validator
    if not file2check2: # not paired reads
        if file2check and validate_input_file(file2check):
            exit(f'validated one file = {file2check}')
    else:
        if file2check and validate_input_file(file2check) and validate_input_file(file2check2):
            exit(f'validated files = {file2check}, {file2check2}')
    if not file2check2:
        print(f'validation failed {file2check}, deleting folder')
    else:
        print(f'validation failed file1: {file2check} file2: {file2check2}, deleting folder')
    delete_folder(parent_folder)
