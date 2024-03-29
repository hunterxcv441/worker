import socket
import pandas as pd
import json
import threading
from queue import Queue
import os

# Configurações iniciais
server_ip = "18.119.115.193"
server_port = 3327
excel_file_path = 'base.xlsx'  # Caminho para o arquivo XLSX de entrada
output_excel_file_path = 'enderecos_atualizados.xlsx'  # Caminho para o arquivo XLSX de saída
progress_file_path = 'progress.txt'  # Caminho para o arquivo de progresso
save_interval = 1000  # Salvar a cada 100 endereços processados

# Carregar o DataFrame do arquivo XLSX
df = pd.read_excel(excel_file_path)

# Fila de tarefas
tasks_queue = Queue()

# Carregar ou inicializar o progresso
if os.path.exists(progress_file_path):
    with open(progress_file_path, 'r') as f:
        start_row = int(f.readline().strip())
else:
    start_row = 0

# Função para formatar o endereço
def format_address(row):
    endereco_formatado = f"{row['tipo_logradouro']} {row['logradouro']}, {row['numero']}, {row['uf']}, {row['cep']}, Brasil"
    mun_formatado = f"{row['municipio_descricao']}"
    endereco_formatado = ", ".join(filter(None, map(str.strip, endereco_formatado.split(","))))
    return endereco_formatado, mun_formatado

# Adicionar tarefas à fila
for i, row in df.iloc[start_row:].iterrows():
    formatted_address, _ = format_address(row)
    tasks_queue.put((i, formatted_address, row['municipio_descricao']))

# Função para lidar com a conexão de um worker
def handle_worker(worker_socket, address):
    global start_row
    while not tasks_queue.empty():
        index, formatted_address, municipio = tasks_queue.get()
        try:
            # Enviar tarefa para o worker
            task = {"endereco": formatted_address, "municipio": municipio}
            worker_socket.send(json.dumps(task).encode())

            # Receber resposta do worker
            response_data = worker_socket.recv(1024).decode()
            response = json.loads(response_data)

            # Processar resposta e atualizar DataFrame
            df.at[index, 'latitude'] = response['latitude']
            df.at[index, 'longitude'] = response['longitude']

            # Imprimir progresso
            print(f"Worker {address}: {formatted_address} | Latitude: {response['latitude']} | Longitude: {response['longitude']}")

            # Salvar progresso a cada save_interval endereços processados
            if (index - start_row) % save_interval == 0:
                save_progress(index)

        except Exception as e:
            print(f"Erro com o worker {address}: {e}")
            tasks_queue.put((index, formatted_address, municipio))  # Colocar a tarefa de volta na fila se houver erro
        finally:
            tasks_queue.task_done()

    worker_socket.close()
    print(f"Worker {address} desconectado.")

# Função para salvar o progresso
def save_progress(current_index):
    df.to_excel(output_excel_file_path, index=False)
    with open(progress_file_path, 'w') as f:
        f.write(str(current_index))
    print(f"Progresso salvo em {output_excel_file_path} e linha {current_index} no {progress_file_path}.")

# Inicializar servidor
server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.bind((server_ip, server_port))
server_socket.listen()
print(f"Servidor ouvindo em {server_ip}:{server_port}")

# Aceitar conexões dos workers e iniciar uma thread para cada um
try:
    while True:
        client_socket, addr = server_socket.accept()
        print(f"Conectado ao worker {addr}")
        worker_thread = threading.Thread(target=handle_worker, args=(client_socket, addr))
        worker_thread.start()

except KeyboardInterrupt:
    print("Servidor interrompido pelo usuário.")

finally:
    server_socket.close()
    save_progress(start_row + tasks_queue.qsize())  # Salvar o progresso final
    print("Servidor fechado.")

# Esperar todas as tarefas serem processadas
tasks_queue.join()
