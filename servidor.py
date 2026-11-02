import socket
import threading
import json
import logging
import queue

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Cola thread-safe que simula el broker de mensajería para delegar a workers
cola_tareas = queue.Queue()

def worker(id_worker):
    """Consume tareas de la cola y ejecuta la simulación del procesamiento."""
    while True:
        conexion, tarea = cola_tareas.get()
        try:
            logging.info(f"[Worker {id_worker}] Procesando tarea: {tarea.get('id')}")
            
            # Acá se implementaría la interacción real con PostgreSQL y S3
            resultado = {
                "id_tarea": tarea.get("id"),
                "estado": "completado",
                "resultado": f"Procesado exitosamente por Worker {id_worker}"
            }
            
            # Se devuelve el resultado directamente al cliente por el socket
            conexion.sendall(json.dumps(resultado).encode('utf-8'))
        except Exception as e:
            logging.error(f"[Worker {id_worker}] Error procesando tarea: {e}")
        finally:
            conexion.close()
            cola_tareas.task_done()

def manejar_cliente(conexion, direccion):
    """Recibe la petición del socket, valida y la encola."""
    logging.info(f"Conexión entrante desde {direccion}")
    try:
        datos = conexion.recv(4096)
        if datos:
            tarea = json.loads(datos.decode('utf-8'))
            cola_tareas.put((conexion, tarea))
            logging.info(f"Tarea {tarea.get('id')} enviada a la cola.")
        else:
            conexion.close()
    except json.JSONDecodeError:
        logging.error(f"Error de formato JSON recibido desde {direccion}")
        conexion.close()
    except Exception as e:
        logging.error(f"Error de red con {direccion}: {e}")
        conexion.close()

def iniciar_servidor(host='127.0.0.1', puerto=5000, num_workers=3):
    """Inicializa el socket del orquestador y levanta los workers en background."""
    for i in range(num_workers):
        hilo = threading.Thread(target=worker, args=(i+1,), daemon=True)
        hilo.start()
        
    servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    servidor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    
    try:
        servidor.bind((host, puerto))
        servidor.listen(5)
        logging.info(f"Orquestador escuchando en {host}:{puerto} con {num_workers} workers en pool.")
        
        while True:
            conexion, direccion = servidor.accept()
            hilo_cliente = threading.Thread(target=manejar_cliente, args=(conexion, direccion))
            hilo_cliente.start()
            
    except KeyboardInterrupt:
        logging.info("Apagando el servidor por consola...")
    except Exception as e:
        logging.error(f"Error crítico en el servidor: {e}")
    finally:
        servidor.close()

if __name__ == "__main__":
    iniciar_servidor()