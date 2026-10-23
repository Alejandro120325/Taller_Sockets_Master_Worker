<div align="center">

# 🌐 TALLER DE SOCKETS
### Arquitectura Master–Worker · Sistemas Distribuidos

**Python 3.10+** · **UDP Heartbeats** · **TCP Multihilo** · **JSON Lines** · **Orquestación de tareas**

</div>

> **Propósito académico.** Proyecto basado en el PDF “TALLER DE SOCKETS” de Sistemas Distribuidos. La prueba de estrés está **limitada a un servidor de eco dentro de cada Worker (`127.0.0.1`)**, con dos clientes concurrentes y aproximadamente 8 solicitudes por segundo por Worker. **No genera una ráfaga masiva hacia una dirección de terceros**, por lo que la Actividad 3 está implementada como una **simulación acotada**. Si el docente exige un blanco común, acuerden un entorno de laboratorio aislado y expresamente autorizado antes de adaptarlo.

---

## 📌 1. Qué construimos

```text
                         ┌────────────────────────────────┐
                         │          MASTER.PY             │
                         │                                │
                         │  UDP :5001  Recibe PING / ACK  │
                         │  TCP :5000  Envia JOB (JSON)   │
                         │  Monitor    Tiempo limite 6 s  │
                         │  CLI        Control por consola│
                         └──────────────┬─────────────────┘
                                        │
                ┌───────────────────────┼───────────────────────┐
                │                       │                       │
                ▼                       ▼                       ▼
        ┌───────────────┐       ┌───────────────┐       ┌───────────────┐
        │  WORKER 1     │       │  WORKER 2     │       │  WORKER 3     │
        │ UDP cada 2 s  │       │ UDP cada 2 s  │       │ UDP cada 2 s  │
        │ TCP conectado │       │ TCP conectado │       │ TCP conectado │
        │ Hilos tareas  │       │ Hilos tareas  │       │ Hilos tareas  │
        │ Eco LOCAL     │       │ Eco LOCAL     │       │ Eco LOCAL     │
        └───────────────┘       └───────────────┘       └───────────────┘
```

**Tecnologías:** `socket` (red TCP/UDP), `threading` (tareas simultáneas), `json` (mensajes estructurados), `time.monotonic` (medición de tiempos), `psutil` (CPU), `logging` (logs), `concurrent.futures.ThreadPoolExecutor` (simulación de carga local).

## 📁 2. Archivos

```text
Taller_Sockets_Master_Worker/
├── master.py                  # Servidor central y comandos del operador
├── worker.py                  # Agente: UDP, TCP, tareas y eco local
├── protocolo.py               # Serialización y delimitación TCP
├── requirements.txt           # psutil para consumo real de CPU
├── README.md                  # Este manual
├── GUIA_DEFENSA.md            # Explicación para defender la práctica
├── .gitignore
├── logs/                      # Archivos generados durante la ejecución
└── tests/
    ├── test_protocolo.py      # Validación del protocolo
    └── test_integracion.py    # Prueba de dos Workers + Master
```

## ⚙️ 3. Instalación en Windows 11 (PyCharm / PowerShell)

Abre la carpeta en PyCharm y usa la Terminal integrada.

```powershell
py -3 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

**Opción A — una sola computadora:** abre **tres terminales diferentes**, en esta secuencia:

**Terminal 1 — Master:**

```powershell
.\.venv\Scripts\python.exe master.py
```

**Terminal 2 — Worker 1:**

```powershell
.\.venv\Scripts\python.exe worker.py --id worker1 --master 127.0.0.1
```

**Terminal 3 — Worker 2:**

```powershell
.\.venv\Scripts\python.exe worker.py --id worker2 --master 127.0.0.1
```

> Usa IDs **distintos**. Si repites un ID, la nueva sesión TCP reemplaza a la anterior.

## 💻 4. Dos o más computadoras en la red Ethernet (LAN)

1. En la computadora Master ejecuta `ipconfig` y ubica **Adaptador Ethernet → Dirección IPv4**. No tomes la IP Wi-Fi cuando estés haciendo las pruebas por Ethernet.
2. Todos los equipos deben poder comunicarse dentro de la misma LAN o red virtual.
3. En Windows, permite **TCP entrante 5000** y **UDP entrante 5001** en el firewall para la red de laboratorio. Preferible permitir estos puertos solo para la red privada y equipos conocidos; **no desactives todo el firewall**.
4. Inicia primero el Master: `python master.py --host 0.0.0.0` (o el ejecutable de `.venv`).
5. En cada otra computadora, ejecuta el Worker con la **IP real del Ethernet del Master**:

```powershell
.\.venv\Scripts\python.exe worker.py --id pc_amigo --master 192.168.1.20
```

⚠️ `192.168.1.20` es **solo un ejemplo**. Sustitúyela por la IPv4 que muestre `ipconfig` en ese momento. `0.0.0.0` es para que el Master escuche en sus interfaces; **nunca es la dirección de destino del Worker**.

En Ubuntu/Linux (otro equipo):

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python3 worker.py --id ubuntu_01 --master 192.168.1.20
```

## 🎮 5. Comandos de consola del Master

Teclea estos comandos **únicamente en la terminal del Master**:

| Comando | Para qué sirve |
|---|---|
| `ayuda` | Muestra el menú |
| `nodos` | Tabla global con estado UDP, TCP, CPU y antigüedad del latido |
| `info worker1` | Envía tarea TCP y recibe datos del SO y Python |
| `scan worker1` | Comprueba **solo el puerto local de eco** del Worker |
| `logs worker1` | Solicita los últimos eventos guardados por el Worker |
| `stress 3` | Coordina una prueba local de 3 segundos en todos los Workers activos |
| `BROADCAST_STRESS:127.0.0.1:3` | Forma educativa alternativa del comando global (no acepta IP externa) |
| `salir` | Cierra TCP, UDP y el programa Master |

**Ejemplo (Master):**

```text
master> nodos
NODO               UDP          TCP       CPU      ÚLTIMO PING
worker1            ACTIVO       SÍ        12.5%    0.4 s
worker2            ACTIVO       SÍ        23.1%    1.0 s

master> info worker1
Comando enviado
[RESULTADO] worker1 | 7cb69d38 | {'node_id': 'worker1', 'os': 'Windows-...', ...}

master> stress 3
[BROADCAST] Trabajo a1b2c3d4 enviado a 2/2 Workers; inicio aprox. en 3 s
[RESULTADO] worker1 | a1b2c3d4 | {'mode': 'LOCAL_LOOPBACK_SAFE', 'ok_requests': 24, ...}
[RESULTADO] worker2 | a1b2c3d4 | {'mode': 'LOCAL_LOOPBACK_SAFE', 'ok_requests': 24, ...}
```

Los valores son **ilustrativos** y dependen de los equipos y la ejecución real.

## 📡 6. Protocolo UDP — Heartbeats

**Cada 2 segundos** el Worker envía esta trama (texto UTF-8):

```text
PING:worker1|18.7|READY
```

Significa: `PING:<node_id>|<cpu_usage>|<status>`.

El Master contesta:

```text
ACK:worker1
```

El Master guarda el identificador, IP/puerto de origen UDP, porcentaje de CPU, estado y `last_seen`. Si pasan **6 segundos** sin otro PING, lo marca `CAÍDO` automáticamente. Si el Worker vuelve a reportarse, pasa a `ACTIVO`.

> UDP no garantiza la entrega. La pérdida temporal de datagramas puede producir una caída aparente. El timeout indicado forma parte de la práctica.

## 🔌 7. Protocolo TCP — JSON Lines

Cada trama es un **objeto JSON en UTF-8 terminado en salto de línea `\n`**. El delimitador permite separar mensajes consecutivos dentro del flujo de bytes TCP.

**Registro inicial:**

```json
{"type":"REGISTER","node_id":"worker1"}
```

**Respuesta del Master:**

```json
{"type":"REGISTER_ACK","node_id":"worker1"}
```

**Orden (Master → Worker):**

```json
{"type":"JOB","job_id":"abc123","action":"SYSTEM_INFO"}
```

**Resultado (Worker → Master):**

```json
{"type":"RESULT","node_id":"worker1","job_id":"abc123","action":"SYSTEM_INFO","data":{"os":"Windows","python":"3.12"}}
```

**Orden global acotada (Master → todos los Workers activos):**

```json
{"type":"JOB","job_id":"def456","action":"STRESS_LOCAL","target":"127.0.0.1","duration":3,"start_at":1790000000.0}
```

`start_at` es una marca de tiempo UTC de referencia para coordinar el inicio. Los equipos deberían tener sus relojes sincronizados para evitar diferencias. `job_id` permite relacionar el resultado con la orden. El JSON se limita a **64 KiB por trama**.

## 🧪 8. Cómo demostrar los cinco criterios de la rúbrica

### ✅ Actividad 1 — UDP y caída de un nodo

1. Inicia Master y dos Workers.
2. En Master ejecuta `nodos` y captura el estado `ACTIVO` de ambos.
3. Cierra abruptamente **solo un Worker** (botón de cerrar terminal o finalizar su proceso desde el Administrador de tareas).
4. Espera unos 6 segundos desde el **último PING** de ese Worker. El Master mostrará `CAÍDO/DESCONECTADO`.
5. Ejecuta de nuevo `nodos`. El otro Worker permanece `ACTIVO`.
6. Reinicia el Worker caído con el mismo ID: aparecerá `RECUPERADO`.

### ✅ Actividad 2 — TCP concurrente

Con varios Workers conectados, lanza `info worker1` y `info worker2`. Las conexiones están atendidas por hilos independientes. `logs worker1` obtiene telemetría.

### ✅ Actividad 3 — Simulación sincronizada y limitada

Ejecuta `stress 3`: el Master genera una misma orden con `job_id` y `start_at` para los Workers conectados. Cada Worker espera al instante acordado y abre **como máximo 2 clientes TCP locales**, que envían mensajes pequeños a **su propio servidor de eco `127.0.0.1`**. Luego reporta solicitudes exitosas, errores, latencia media, latencia p95 y solicitudes/s.

**Importante:** esto no demuestra que todos los Workers generen una ráfaga masiva **contra una única IP compartida**. Esa parte literal del enunciado no está incluida por razones de seguridad de red.

### ✅ Actividad 4 — Mensajes

Toma una captura de `PING`, `ACK`, del JSON de registro y de los objetos `JOB`/`RESULT` (también puedes usar los ejemplos de este README como explicación del protocolo).

### ✅ Actividad 5 — Errores y cierre

Detén y reinicia el Master para demostrar reintentos automáticos del Worker. Ejecuta `salir` y comprueba que las conexiones se cierren. Consulta los registros en `logs/`.

## 🧪 9. Pruebas automatizadas

Ejecuta en la carpeta raíz:

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
```

O en Linux:

```bash
python3 -m unittest discover -s tests -v
```

Se valida: formato de mensajes, IDs, registro de dos Workers por UDP/TCP, coordinación de la prueba local y generación de resultados.

## 🛠️ 10. Problemas habituales

| Problema | Comprobación |
|---|---|
| `ConnectionRefusedError` | Master no iniciado / IP o puerto TCP incorrectos |
| `Sin ACK UDP` | Puerto UDP 5001 bloqueado / dirección Master incorrecta |
| Aparece `0.0.0.0` en Master | Es normal: indica **escuchar en todas las interfaces**, no una IP para clientes |
| Dos Workers usan el mismo ID | Cambia `--id`; los identificadores deben ser únicos |
| Worker muestra `CAÍDO`, pero TCP sigue SÍ | El monitoreo UDP es **independiente** de la conexión TCP |
| CPU reporta 0 % | `psutil` no instalado o la muestra es demasiado corta; instala requisitos |
| `Tiempo de inicio inválido` | Sincroniza la hora en Windows/Linux; el `start_at` es común |
| Puerto en uso (`Address already in use`) | Detén el Master anterior o cambia ambos puertos en Master y Workers |

## 🔒 11. Alcance y limitaciones

- Laboratorio para redes privadas y equipos propios/autorizados.
- El protocolo **no** implementa autenticación, TLS ni cifrado: no lo expongas a Internet.
- La carga se ejecuta **solo en loopback** del Worker; no existe selección arbitraria de víctimas ni ejecución de comandos del sistema (`shell`).
- `SCAN_LOCAL_PORT` consulta solo el puerto efímero del eco propio; no efectúa barridos de puertos externos.
- Máximo: **10 segundos**, **2 conexiones concurrentes** y ~**8 solicitudes por segundo por Worker**.
- Para entregar, incluye capturas auténticas o logs de **tu ejecución**. No presentes los ejemplos ilustrativos como evidencia real.

---

**Taller de Sistemas Distribuidos · Python · Master–Worker · UDP/TCP**
