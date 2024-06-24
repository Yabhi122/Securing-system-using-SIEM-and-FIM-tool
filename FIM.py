import os
import logging
import hashlib
import time
import openpyxl
import shutil
from PIL import Image
from docx import Document
import uuid
from PyPDF2 import PdfFileReader
from pptx import Presentation
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import schedule
import threading
from datetime import datetime
import glob

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def schedule_backup(target_folders, backup_location):
    """
    Schedule backup function to run every minute.
    """
    schedule.every(1).minutes.do(backup_and_manage, target_folders=target_folders, backup_location=backup_location)
    while True:
        schedule.run_pending()
        time.sleep(1)

def backup_folders(target_folders, backup_location):
    """
    Backup target folders to a specified location.

    Args:
        target_folders: A list of paths to the target folders.
        backup_location: The path to the location where backups will be stored.
    """
    # Create a timestamp for the backup folder name
    timestamp = time.strftime("%Y%m%d%H%M%S")
    backup_folder = os.path.join(backup_location, f"backup_{timestamp}")

    # Create the backup folder
    os.makedirs(backup_folder)

    # Copy files from target folders to the backup folder
    for folder in target_folders:
        folder_name = os.path.basename(folder)
        destination = os.path.join(backup_folder, folder_name)
        shutil.copytree(folder, destination)

    print("Backup completed successfully.")

    # Log backup creation
    logger.info(f"Backup created at: {backup_folder}")

    return backup_folder

def delete_old_backups(backup_location):
    """
    Delete old backups from the specified location, keeping a specified number of recent backups and ensuring a maximum backup age.

    Args:
        backup_location: The path to the location where backups are stored.
    """
    # Get a list of all backup folders
    backups = glob.glob(os.path.join(backup_location, "*"))
    backups.sort(key=os.path.getctime)

    # Delete oldest backup if there are more than 2 backups
    if len(backups) > 2:
        oldest_backup = backups[0]
        shutil.rmtree(oldest_backup)
        logging.info(f"Deleted old backup: {oldest_backup}")


def backup_and_manage(target_folders, backup_location):
    """ Function to backup folders, delete old backups, and manage backups periodically. """
    try:
        # Backup the folders
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        backup_folder = os.path.join(backup_location, timestamp)

        # Check if the backup folder already exists
        if not os.path.exists(backup_folder):
            # Create the backup folder
            os.makedirs(backup_folder)

            # Copy files from target folders to the backup folder
            for folder in target_folders:
                folder_name = os.path.basename(folder)
                destination = os.path.join(backup_folder, folder_name)
                shutil.copytree(folder, destination)

            print("Backup completed successfully.")

            # Log backup creation
            logging.info(f"Backup created at: {backup_folder}")

            # Delete old backups
            delete_old_backups(backup_location)
        else:
            # Log a message indicating that the backup folder already exists
            pass

    except FileExistsError:
        # Log the specific error without printing it
        pass

    except Exception as e:
        logging.exception("An error occurred in backup_and_manage function: %s", e)


def process_file_changes(filename, event_id):
    """
    Processes changes in a file by comparing it with the baseline and copies the original file to the safe folder.
    
    Args:
    filename: The path to the file to be processed.
    event_id: The unique event ID associated with the file event.
    safe_folder_path: The path to the safe folder where the original files will be copied.
    """
    logger = logging.getLogger(__name__)
    
    try:
        # Load the baseline data
        baseline_data = {}
        with open("baseline.txt", "r") as f:
            for line in f:
                parts = line.strip().split("|")
                if len(parts) >= 3:
                    path = parts[0]
                    file_hash = parts[1]
                    event_id = parts[2]
                    baseline_data[path] = {"hash": file_hash, "event_id": event_id}

        # Calculate hash of the current file
        current_hash = calculate_file_hash(filename)
        
        # Compare with baseline
        if filename not in baseline_data:
            logger.info(f"101 File at path: {filename}, Action: New file detected.")
        elif current_hash != baseline_data[filename]["hash"]:
            logger.info(f"103 File at path: {filename}, Action: File changed.")
        else:
            logger.info(f"100 File at path: {filename}, Action: No change in file.")
        
        # Update baseline data with new hash
        baseline_data[filename] = {"hash": current_hash, "event_id": event_id}
        
        # Save updated baseline data
        with open("baseline.txt", "w") as f:
            for path, info in baseline_data.items():
                f.write(f"{path}|{info['hash']}|{info['event_id']}\n")
    
    except Exception as e:
        logger.error(f"Error processing changes in {filename}: {e}")



        
def process_image_changes(filename, event_id):
    """
    Processes changes in an image file by comparing it with the baseline.

    Args:
        filename: The path to the image file to be processed.
        event_id: The unique event ID associated with the file event.
    """
    logger = logging.getLogger(__name__)
    try:
        # Load the baseline data
        baseline_data = {}
        with open("baseline.txt", "r") as f:
            for line in f:
                parts = line.strip().split("|")
                if len(parts) >= 3:
                    path = parts[0]
                    file_hash = parts[1]
                    event_id = parts[2]
                    baseline_data[path] = {"hash": file_hash, "event_id": event_id}

        # Calculate hash of the current image
        current_hash = calculate_file_hash(filename)

        # Compare with baseline
        if filename not in baseline_data:
            logger.info(f"101 File at path: {filename}, Action: New image detected.")
        elif current_hash != baseline_data[filename]["hash"]:
            logger.info(f"103 File at path: {filename}, Action: Image changed.")
        else:
            logger.info(f"100 File at path: {filename}, Action: No change in image.")

        # Update baseline data with new hash
        baseline_data[filename] = {"hash": current_hash, "event_id": event_id}

        # Save updated baseline data
        with open("baseline.txt", "w") as f:
            for path, info in baseline_data.items():
                f.write(f"{path}|{info['hash']}|{info['event_id']}\n")

    except Exception as e:
        logger.error(f"Error processing changes in {filename}: {e}")

def process_excel_changes(filename, event_id):
    """
    Processes changes in an Excel file by comparing it with the baseline.

    Args:
        filename: The path to the Excel file to be processed.
        event_id: The unique event ID associated with the file event.
    """
    logger = logging.getLogger(__name__)
    try:
        # Load the baseline data
        baseline_data = {}
        with open("baseline.txt", "r") as f:
            for line in f:
                parts = line.strip().split("|")
                if len(parts) >= 3:
                    path = parts[0]
                    file_checksum = parts[1]
                    event_id = parts[2]
                    baseline_data[path] = {"checksum": file_checksum, "event_id": event_id}

        # Calculate checksum of the current Excel file
        current_checksum = calculate_file_checksum(filename)

        # Compare checksum with the baseline
        if filename not in baseline_data:
            logger.info(f"101 File at path: {filename}, Action: New Excel file detected.")
        elif current_checksum != baseline_data[filename]["checksum"]:
            logger.info(f"103 File at path: {filename}, Action: Excel file has modified.")
        else:
            logger.info(f"100 File at path: {filename}, Action: No change in Excel file.")

        # Update baseline data with new checksum
        baseline_data[filename] = {"checksum": current_checksum, "event_id": event_id}

        # Save updated baseline data
        with open("baseline.txt", "w") as f:
            for path, info in baseline_data.items():
                f.write(f"{path}|{info['checksum']}|{info['event_id']}\n")

    except Exception as e:
        logger.error(f"Error processing changes in {filename}: {e}")

def calculate_file_checksum(filepath):
    """Calculates the checksum of a file.
    
    Args:
        filepath: The path to the file.
        
    Returns:
        A string containing the checksum of the file.
    """
    with open(filepath, "rb") as f:
        data = f.read()
        return hashlib.sha256(data).hexdigest()
    
def process_word_changes(filename, event_id):
    logger = logging.getLogger(__name__)
    try:
        if filename.startswith('~$'):
            # Skip temporary Word files
            return

        # Load the baseline data
        baseline_data = {}
        with open("baseline.txt", "r") as f:
            for line in f:
                parts = line.strip().split("|")
                if len(parts) >= 3:
                    path = parts[0]
                    file_hash = parts[1]
                    file_event_id = parts[2]
                    baseline_data[path] = {"hash": file_hash, "event_id": file_event_id}

        # Check if the file exists
        if not os.path.exists(filename):
            if filename in baseline_data:
                logger.info(f"102 File at path: {event_id} {filename}, Action: Word document has been deleted.")
                del baseline_data[filename]
            else:
                logger.warning(f"*** {event_id}] [{filename}] Word document deletion event occurred but not tracked.")
            return

        # Calculate hash of the current Word document
        current_hash = calculate_file_hash(filename)

        # Check if the file is not a temporary Word file and not in baseline data
        if not filename.startswith('~$') and filename not in baseline_data:
            logger.info(f"101 File at path: {event_id} {filename}, Action: New Word document detected.")
        elif filename in baseline_data and current_hash != baseline_data[filename]["hash"]:
            logger.info(f"103 File at path: {filename}, Action: Word document changed.")
        elif filename in baseline_data:
            logger.info(f"100 File at path: {event_id} {filename}, Action: No change in Word document.")

        # Update baseline data with new hash and event_id
        baseline_data[filename] = {"hash": current_hash, "event_id": event_id}

        # Save updated baseline data
        with open("baseline.txt", "w") as f:
            for path, info in baseline_data.items():
                f.write(f"{path}|{info['hash']}|{info['event_id']}\n")

    except Exception as e:
        logger.error(f"Error processing changes in {filename}: {e}")


def process_pdf_changes(filename, event_id):
    """
    Processes changes in a PDF file by comparing it with the baseline.

    Args:
        filename: The path to the PDF file to be processed.
        event_id: The unique event ID associated with the file event.
    """
    logger = logging.getLogger(__name__)
    try:
        # Load the baseline data
        baseline_data = {}
        with open("baseline.txt", "r") as f:
            for line in f:
                parts = line.strip().split("|")
                if len(parts) >= 3:
                    path = parts[0]
                    file_hash = parts[1]
                    event_id = parts[2]
                    baseline_data[path] = {"hash": file_hash, "event_id": event_id}

        # Calculate hash of the current PDF file
        current_hash = calculate_file_hash(filename)

        # Compare with baseline
        if filename not in baseline_data:
            logger.info(f"101 File at path: {filename}, Action: New PDF document detected.")
        elif current_hash != baseline_data[filename]["hash"]:
            logger.info(f"103 File at path: {filename}, Action: PDF document changed.")
        else:
            logger.info(f"100 File at path: {filename}, Action: No change in PDF document.")

        # Update baseline data with new hash
        baseline_data[filename] = {"hash": current_hash, "event_id": event_id}

        # Save updated baseline data
        with open("baseline.txt", "w") as f:
            for path, info in baseline_data.items():
                f.write(f"{path}|{info['hash']}|{info['event_id']}\n")

    except Exception as e:
        logger.error(f"Error processing changes in {filename}: {e}")

def process_text_changes(filename, event_id):
    """
    Processes changes in a text file by comparing it with the baseline.

    Args:
        filename: The path to the text file to be processed.
        event_id: The unique event ID associated with the file event.
    """
    logger = logging.getLogger(__name__)
    try:
        # Load the baseline data
        baseline_data = {}
        with open("baseline.txt", "r") as f:
            for line in f:
                parts = line.strip().split("|")
                if len(parts) >= 3:
                    path = parts[0]
                    file_hash = parts[1]
                    event_id = parts[2]
                    baseline_data[path] = {"hash": file_hash, "event_id": event_id}

        # Calculate hash of the current text file
        current_hash = calculate_file_hash(filename)

        # Compare with baseline
        if filename not in baseline_data:
            logger.info(f"101 File at path: {filename}, Action: New text file detected.")
        elif current_hash != baseline_data[filename]["hash"]:
            logger.info(f"103 File at path: {filename}, Action: Text file changed.")
        else:
            logger.info(f"100 File at path: {filename}, Action: No change in text file.")

        # Update baseline data with new hash
        baseline_data[filename] = {"hash": current_hash, "event_id": event_id}

        # Save updated baseline data
        with open("baseline.txt", "w") as f:
            for path, info in baseline_data.items():
                f.write(f"{path}|{info['hash']}|{info['event_id']}\n")

    except Exception as e:
        logger.error(f"Error processing changes in {filename}: {e}")

def check_file_type(filename):
    """
    Checks the type of the file.

    Args:
        filename: The path to the file.

    Returns:
        'excel' if the file is an Excel file, 'image' if it's an image file,
        'word' if it's a Word document, 'pdf' if it's a PDF document,
        'pptx' if it's a PowerPoint document, None otherwise.
    """
    if filename.endswith('.xlsx'):
        return 'excel'
    elif filename.endswith(('.jpg', '.jpeg', '.png', '.gif')):
        return 'image'
    elif filename.endswith('.docx'):
        return 'word'
    elif filename.endswith('.pdf'):
        return 'pdf'
    elif filename.endswith('.pptx'):
        return 'pptx'
    return None

def process_file(filename, event_id):
    """
    Processes a file based on its type.

    Args:
        filename: The path to the file to be processed.
        event_id: The unique event ID associated with the file event.
        safe_folder_path: The path to the safe folder where the original files will be copied.
    """
    file_type = check_file_type(filename)
    logger = logging.getLogger(__name__)
    try:
        # Process the file based on its type
        if file_type == 'excel':
            process_excel_changes(filename, event_id)
        elif file_type == 'image':
            process_image_changes(filename, event_id)
        elif file_type == 'word':
            process_word_changes(filename, event_id)
        elif file_type == 'pdf':
            process_pdf_changes(filename, event_id)
        elif file_type == 'txt':
            process_text_changes(filename, event_id)  # Add this line to handle text files
        else:
            # Handle other file types here
            logger.info(f"103 File at path: {filename}, Action: File has been changed.")
    except Exception as e:
        logger.error(f"Error processing {filename}: {e}")

def calculate_file_hash(filepath):
    """Calculates the SHA512 hash of a file.
    
    Args:
        filepath: The path to the file.
        
    Returns:
        A string containing the SHA512 hash of the file.
    """
    with open(filepath, "rb") as f:
        data = f.read()
        return hashlib.sha512(data).hexdigest()

def erase_existing_baseline():
    """
    Deletes the "baseline.txt" file if it exists.
    """
    if os.path.exists("baseline.txt"):
        os.remove("baseline.txt")

def collect_baseline(target_folders):
    """
    Collects baseline information for files in the target folders and their subfolders.

    Args:
        target_folders: A list of paths to the target folders.
    """
    erase_existing_baseline()

    # Collect baseline information for each target folder
    for target_folder in target_folders:
        # Initialize file_info_dict for this target folder
        file_info_dict = {}

        # Collect all files in the target folder and its subfolders
        for root, dirs, files in os.walk(target_folder):
            for f in files:
                full_path = os.path.join(root, f)
                
                # Skip files starting with '$' or '~$'
                if f.startswith('$') or f.startswith('~$'):
                    continue

                file_hash = calculate_file_hash(full_path)
                event_id = str(uuid.uuid4())  # Generate a UUID for the event
                file_info_dict[full_path] = {"hash": file_hash, "event_id": event_id}

        # Save the dictionary to "baseline.txt"
        with open("baseline.txt", "a") as f:  # Use 'a' (append) mode to add to existing baseline
            for path, info in file_info_dict.items():
                f.write(f"{path}|{info['hash']}|{info['event_id']}\n")

def monitor_files(target_folders):
    """
    Monitor changes in files within the specified target folders and their subfolders.

    Args:
        target_folders: A list of paths to the target folders.
        safe_folder_path: The path to the safe folder where the original files will be copied.
    """
    file_info_dict = {}  # Initialize an empty dictionary to store file information
    excluded_dirs = ['image files']  # Directories to exclude from monitoring
    
    try:
        with open("baseline.txt", "r") as f:
            for line in f:
                parts = line.strip().split("|")
                if len(parts) >= 3:
                    path = parts[0]
                    file_hash = parts[1]
                    event_id = parts[2]
                    file_info_dict[path] = {"hash": file_hash, "event_id": event_id, "path": path}  # Added "path" key
    except Exception as e:
        print(f"Error loading baseline: {e}")

    while True:
        time.sleep(5)  # Delay for monitoring
        
        for target_folder in target_folders:
            for root, dirs, files in os.walk(target_folder):
                # Exclude certain directories from being processed
                dirs[:] = [d for d in dirs if d not in excluded_dirs]
                
                for f in files:
                    full_path = os.path.join(root, f)
                    
                    try:
                        with open(full_path, "rb") as file_handle:
                            pass
                    except PermissionError:
                        print(f"\n{full_path} is in use, skipping...")
                        continue
                    
                    # Check if the file is new (not in the baseline) and doesn't start with '~$'
                    if full_path not in file_info_dict:
                        event_id = str(uuid.uuid4())  # Generate a UUID for the event
                        file_hash = calculate_file_hash(full_path)
                        file_info_dict[full_path] = {"hash": file_hash, "event_id": event_id, "path": full_path}  # Added "path" key
                        
                        # Log new file creation event
                        logger = logging.getLogger(__name__)
                        logger.info(f"101 File at path: {full_path}, Action: New file detected.")

                    # Update baseline information for existing files
                    else:
                        current_hash = calculate_file_hash(full_path)
                        
                        # Check if the file has been modified
                        if current_hash != file_info_dict[full_path]["hash"]:
                            event_id = str(uuid.uuid4())  # Generate a UUID for the event
                            file_info_dict[full_path]["hash"] = current_hash
                            file_info_dict[full_path]["event_id"] = event_id
                            process_file(full_path, event_id)  # Pass safe_folder_path here
                            continue  # Skip further checks if file has been modified

                        # Check for rename operation
                        if full_path != file_info_dict[full_path]["path"]:
                            # Log file renaming event
                            logger = logging.getLogger(__name__)
                            logger.info(f"104 File at path: {file_info_dict[full_path]['path']}, Action: File has been renamed to {full_path}")
                            
                            # Update baseline data with new path
                            file_info_dict[full_path] = file_info_dict.pop(file_info_dict[full_path]["path"])
                            file_info_dict[full_path]["path"] = full_path
                            continue  # Skip further checks if file has been renamed

                # Check for delete operation
                for path in list(file_info_dict.keys()):
                    if not os.path.exists(path):
                        # Log file deletion event 
                        logger = logging.getLogger(__name__)
                        logger.info(f"102 File at path: {path}, Action: File has been deleted")
                        del file_info_dict[path]



                
    # Add the necessary logging configuration and handlers here

# Create a FileHandler and set its properties
log_file = "folder_logs.log"
file_handler = logging.FileHandler(log_file)
file_handler.setLevel(logging.INFO)

# Create a formatter
formatter = logging.Formatter('Date:%(asctime)s, Time:%(asctime)s, Event_id:%(message)s')


# Set the formatter for the file handler
file_handler.setFormatter(formatter)

# Add the FileHandler to the logger
logger.addHandler(file_handler)

def monitor_files_thread(target_folders):
    """Starts monitoring files in a separate thread."""
    monitor_files(target_folders)


if __name__ == "__main__":
    # Define log file path and target folders
    log_file = "folder_logs.log"
    target_folders = [
        r"C:\Users\vmadmin\Desktop\files"
    ]
    
    # Define backup location
    backup_location = r"C:\Backup"

    # Configure logging
    logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s - %(message)s, Event_id:%(message)s, Path:%(levelname)s')

    # Collect baseline or monitor files based on user input
    while True:
        print("\nWhat would you like to do?")
        print("  A) Collect new Baseline?")
        print("  B) Begin monitoring files with saved Baseline?")
        response = input("\nPlease enter 'A' or 'B': ").upper()

        if response == "A":
            collect_baseline(target_folders)
        elif response == "B":
            # Backup folders and manage backups periodically
            backup_and_manage(target_folders, backup_location)
            
            
            # Start monitoring files in a separate thread
            monitor_thread = threading.Thread(target=monitor_files_thread, args=(target_folders,))
            monitor_thread.daemon = True  # Daemonize the thread to exit when the main program exits
            monitor_thread.start()

            backup_thread = threading.Thread(target=schedule_backup, args=(target_folders, backup_location))
            backup_thread.start()
            
            # Infinite loop to keep the scheduling running
            while True:
                schedule.run_pending()
                time.sleep(1)  # Add a small delay to avoid high CPU usage
        else:
            print("Invalid input. Please enter 'A' or 'B'.")