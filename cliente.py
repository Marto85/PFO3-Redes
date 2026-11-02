import socket
import json
import uuid

def enviar_tarea(host='127.0.0.1', puerto=5000):
    """Envía una tarea al servidor y aguarda el resultado sincrónicamente."""
    cliente = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    try:
        cliente.connect((host, puerto))
        
        tarea = {
            "id": str(uuid.uuid4())[:8], 
            "accion": "analizar_datos", 
            "payload": "lote_001.csv"
        }
        
        print(f"[*] Enviando tarea al orquestador: {tarea}")
        cliente.sendall(json.dumps(tarea).encode('utf-8'))
        
        respuesta_cruda = cliente.recv(4096)
        
        if respuesta_cruda:
            resultado = json.loads(respuesta_cruda.decode('utf-8'))
            print(f"[+] Respuesta recibida: {resultado}")
        else:
            print("[-] El servidor cerró la conexión sin enviar respuesta.")
            
    except ConnectionRefusedError:
        print(f"[!] Error: Conexión rechazada en {host}:{puerto}. Verificá que el servidor esté corriendo.")
    except Exception as e:
        print(f"[!] Error de red: {e}")
    finally:
        cliente.close()

if __name__ == "__main__":
    enviar_tarea()