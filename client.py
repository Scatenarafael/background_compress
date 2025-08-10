import os
import socket
import threading
import time

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

FOLDER = f"{BASE_DIR}/files"

thread_lock = threading.Lock()


def send_file(servidor_ip: str, servidor_porta: int, filename: str):
    # Conecta ao servidor
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect((servidor_ip, servidor_porta))

    print(f"Conectado ao servidor {servidor_ip}:{servidor_porta}")
    path = os.path.join(FOLDER, filename)

    try:
        first_time = True
        # Envia o nome do arquivo para o servidor
        with open(path, "rb") as file:
            while True:
                dados = file.read(1024)
                if not dados:
                    break

                if first_time:
                    concat_data = file.name.split("/")[-1].rjust(35, "0").encode("utf-8") + dados
                    client.send(concat_data)
                else:
                    client.send(dados)
                first_time = False
        for i in range(60):
            time.sleep(1)
            print(f"filename: {filename} - remaining seconds: {60 - i}")
        print(f"Arquivo {filename} enviado com sucesso.\n")

    except Exception as e:
        print(f"[ERRO]: {e}")
        client.close()

    finally:
        # Fecha a conexão
        client.close()


def main():
    source_dir = f"{BASE_DIR}/files"

    files = os.listdir(source_dir)

    # threads = []

    # with thread_lock:
    #     for filename in files:
    #         thread = threading.Thread(target=send_file, args=("127.0.0.1", 5000, filename))

    #         thread.start()

    #         threads.append(thread)

    #     for thread in threads:
    #         thread.join()

    for filename in files:
        send_file("127.0.0.1", 5000, filename)


if __name__ == "__main__":
    main()
