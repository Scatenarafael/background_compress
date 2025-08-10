import os
import socket
import threading

BUFFER_SIZE = 1024

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

FOLDER = f"{BASE_DIR}/monitoring_folder"

thread_lock = threading.Lock()


def handle_receive_files(conn, address):
    print(f"Conexão recebida de {address}")

    initial_msg = conn.recv(35)
    filename = initial_msg.decode("utf-8").rstrip("0").lstrip("0")

    try:
        while True:
            data = conn.recv(BUFFER_SIZE)
            if not data:
                break

            filepath = os.path.join(FOLDER, filename)

            with open(filepath, "wb") as f:
                while True:
                    chunk = conn.recv(BUFFER_SIZE)
                    if not chunk:
                        break
                    f.write(chunk)

            print(f"Arquivo recebido: {filepath}")
    except Exception as e:
        print(f"Erro ao receber arquivo: {e}")
    finally:
        conn.close()


def main():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(("0.0.0.0", 5000))
    server.listen()
    print("Server running...")

    threads = []

    try:
        while True:
            try:
                conn, address = server.accept()

                with thread_lock:
                    thread = threading.Thread(target=handle_receive_files, args=(conn, address))

                    thread.start()
                    threads.append(thread)

                for t in threads:
                    t.join()

            except Exception as e:
                print(f"Erro: {e}")
                conn.close()
    except KeyboardInterrupt:
        print("Servidor encerrado.")
    finally:
        server.close()


if __name__ == "__main__":
    main()
