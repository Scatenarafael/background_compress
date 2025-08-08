# # pylint: disable=missing-module-docstring,missing-class-docstring,missing-function-docstring
# type: ignore

import multiprocessing
import os
import tarfile
import threading
import time
from datetime import datetime

# === CONFIGURAÇÕES ===

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

FOLDER = f"{BASE_DIR}/monitoring_folder/"

RESULT_FOLDER = f"{FOLDER}/compressed/"

LOCKFILE = f"{FOLDER}tmp/compress.lock"


EXTENSION = ".dasf"
THRESHOLD_BYTES = 1 * 1024 * 1024 * 1024  # 10GB
MIN_AGE_SECONDS = 60  # 1 minuto

NUM_PROCESSES = 3
# def is_locked():
#     print("[COMPRESS LOCKED] is_locked: Verificando se a compactação já está em andamento")
#     return os.path.exists(LOCKFILE)


# def create_lock():
#     print("[COMPRESS] LOCKING...")
#     with open(LOCKFILE, "w") as f:
#         f.write(str(os.getpid()))


# def release_lock():
#     if os.path.exists(LOCKFILE):
#         os.remove(LOCKFILE)
#         print("[COMPRESS] LOCKING RELEASED...")

# write_lock = threading.Lock()


# === UTILITÁRIOS ===
def get_dasf_files():
    """
    Retorna uma lista de arquivos .dasf com seus tamanhos e paths,
    desde que tenham mais de MIN_AGE_SECONDS de idade.
    """
    now = time.time()
    valid_files = []

    for root, _, filenames in os.walk(FOLDER):
        for f in filenames:
            if f.endswith(EXTENSION):
                full_path = os.path.join(root, f)
                try:
                    stat = os.stat(full_path)
                    age = now - stat.st_mtime  # modificação (criação não é bem suportada)
                    if age >= MIN_AGE_SECONDS:
                        size = stat.st_size
                        valid_files.append((full_path, size))
                except FileNotFoundError:
                    continue
    print("[TASKS] GET_DASF_FILES")
    print("[TASKS] GET_DASF_FILES: Encontrados %d arquivos .dasf válidos", len(valid_files))
    return valid_files


def compress_files(files, output_tar):
    write_threads = []

    pid = os.getpid()

    with tarfile.open(output_tar, "w:gz") as tar:
        for filepath, _ in files:
            arcname = os.path.relpath(filepath, FOLDER)
            try:
                # with write_lock:
                write_thread = threading.Thread(target=tar.add, args=(filepath, arcname))
                write_threads.append(write_thread)
                write_thread.start()
                print(f"[TASKS] [PID - {pid}] Arquivo adicionado ao tar: {arcname}")

                for thread in write_threads:
                    thread.join()
            except Exception as e:
                print(f"Something went wrong with {filepath}: {e}")


def remove_files(files):
    remove_threads = []

    try:
        for filepath, _ in files:
            remove_thread = threading.Thread(target=os.remove, args=(filepath,))
            remove_threads.append(remove_thread)
            remove_thread.start()

        for thread in remove_threads:
            thread.join()
    except Exception as e:
        print(f"[TASKS] Something went wrong - Could not remove lock file: {e}")

    finally:
        print("[TASKS] check_and_compress_raw_files: All files removed successfully")


def worker(file_batch):
    """Processo que irá compactar um lote de arquivos."""
    timestamp = time.strftime("%Y%m%d%H%M%S")
    pid = os.getpid()

    if not file_batch:
        return

    print(f"PID - {pid} - Processing batch with {file_batch}")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = os.path.join(RESULT_FOLDER, f"compressed_{timestamp}_{pid}.tar.gz")
    compress_files(file_batch, output_file)


def remove_worker(file_batch):
    """Processo que irá compactar um lote de arquivos."""
    if not file_batch:
        return
    remove_files(file_batch)


def check_and_compress_raw_files():
    print("[TASKS] check_and_compress_raw_files: Starting .dasf compression task")
    files = get_dasf_files()
    total_size = sum(size for _, size in files)

    if total_size >= THRESHOLD_BYTES:
        timestamp = time.strftime("%Y%m%d%H%M%S")
        output_tar = os.path.join(FOLDER, f"compressed_{timestamp}.tar.gz")

        process = []

        try:
            # Divide os arquivos em lotes para cada processo
            chunk_size = max(1, len(files) // NUM_PROCESSES)
            file_batches = [files[i : i + chunk_size] for i in range(0, len(files), chunk_size)]

            for batch in file_batches:
                p = multiprocessing.Process(target=worker, args=(batch,))
                p.start()
                process.append(p)

            process = [p.join() for p in process]

            # with multiprocessing.Pool(processes=NUM_PROCESSES) as pool:
            #     pool.map(worker, file_batches)

            print(f"✅ Compressed to {output_tar} ({total_size / 1024**3:.2f} GB).")
        except Exception as e:
            print(f"[TASKS] Something went wrong - Could not start compression: {e}")

        finally:
            print("[TASKS] check_and_compress_raw_files: Compression task completed - lock released")

        try:
            with multiprocessing.Pool(processes=NUM_PROCESSES) as pool:
                pool.map(remove_worker, file_batches)
        except Exception as e:
            print(f"[TASKS] Something went wrong - Could not remove lock file: {e}")

        # finally:
        #     print("[TASKS] check_and_compress_raw_files: All files removed successfully")
