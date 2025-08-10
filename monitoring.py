# type: ignore
# pylint: disable=missing-function-docstring, missing-module-docstring, missing-class-docstring
import os
import time
from pathlib import Path

from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from compress import check_and_compress_raw_files

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

FOLDER = f"{BASE_DIR}/monitoring_folder"


RESULT_FOLDER = f"{FOLDER}/compressed/"

EXTENSION = ".dasf"


class DASFHandler(FileSystemEventHandler):
    def on_created(self, event):
        folder = event.src_path.split("/")[-3]
        print(f"Event created in folder: {folder}")
        if not event.is_directory and event.src_path.endswith(EXTENSION) and folder != "compressed":
            print(f"New file detected: {event.src_path}")
            check_and_compress_raw_files()

        print("This change was detected but the requirements to check and compress .das files were not met.")


def start_monitoring():
    monitoring_folder = Path(FOLDER)
    monitoring_folder.mkdir(parents=True, exist_ok=True)
    result_folder = Path(RESULT_FOLDER)
    result_folder.mkdir(parents=True, exist_ok=True)

    print("BASE_DIR:", BASE_DIR)
    observer = Observer()
    observer.schedule(DASFHandler(), path=FOLDER, recursive=True)
    observer.start()
    print(f"👀 Monitoring {FOLDER} to *{EXTENSION} files")

    try:
        while True:
            time.sleep(5)
    except KeyboardInterrupt:
        observer.stop()
    finally:
        observer.join()
