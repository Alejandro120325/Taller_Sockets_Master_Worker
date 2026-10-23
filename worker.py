"""Nodo Worker: latidos UDP, sesión TCP persistente y tareas asíncronas seguras.

Uso: python worker.py --id worker1 --master 127.0.0.1
"""
from __future__ import annotations

import argparse
import logging
import platform
import socket
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

from protocolo import recv_json, send_json, valid_node_id

try:
    import psutil
except ImportError:
    psutil = None

HEARTBEAT_INTERVAL = 2.0
MAX_STRESS_SECONDS = 10
LOCAL_CONCURRENCY = 2


class Worker:
    def __init__(self, node_id: str, master_host: str = "127.0.0.1", tcp_port: int = 5000,
                 udp_port: int = 5001):
        if not valid_node_id(node_id):
            raise ValueError("ID: 1-32 caracteres alfanuméricos, guiones o guiones bajos")
        self.node_id = node_id
        self.master_host = master_host
        self.tcp_port = tcp_port
        self.udp_port = udp_port
        self.stop_event = threading.Event()
        self.logger = self._logger()
        self.events: deque[str] = deque(maxlen=30)
        self.events_lock = threading.Lock()
        self.tcp_lock = threading.Lock()
        self.tcp_sock: socket.socket | None = None
        self.udp_sock: socket.socket | None = None
        self.echo_sock: socket.socket | None = None
        self.echo_port: int = 0
        self.background: list[threading.Thread] = []
        self.stress_lock = threading.Lock()

    def _logger(self) -> logging.Logger:
        directory = Path(__file__).parent / "logs"
        directory.mkdir(exist_ok=True)
        logger = logging.getLogger(f"taller.worker.{self.node_id}")
        if not logger.handlers:
            logger.setLevel(logging.INFO)
            format_ = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s", "%H:%M:%S")
            for handler in (logging.StreamHandler(), logging.FileHandler(directory / f"{self.node_id}.log", encoding="utf-8")):
                handler.setFormatter(format_)
                logger.addHandler(handler)
        return logger

    def event(self, text: str) -> None:
        message = f"{time.strftime('%H:%M:%S')} | {text}"
        with self.events_lock:
            self.events.append(message)
        self.logger.info(text)

    def start(self) -> None:
        """Inicia un eco TCP EXCLUSIVAMENTE en el propio Worker, no accesible desde LAN."""
        self.echo_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.echo_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.echo_sock.bind(("127.0.0.1", 0))
        self.echo_sock.listen(16)
        self.echo_sock.settimeout(0.5)
        self.echo_port = self.echo_sock.getsockname()[1]
        self.udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_sock.settimeout(0.7)
        for target, name in ((self._echo_loop, "Echo-local"), (self._heartbeat_loop, "UDP-heartbeat")):
            t = threading.Thread(target=target, name=f"{self.node_id}-{name}", daemon=True)
            t.start()
            self.background.append(t)
        self.event(f"Worker iniciado. Eco aislado: 127.0.0.1:{self.echo_port}")

    def _echo_loop(self) -> None:
        assert self.echo_sock is not None
        while not self.stop_event.is_set():
            try:
                conn, _ = self.echo_sock.accept()
                threading.Thread(target=self._echo_one, args=(conn,), daemon=True).start()
            except socket.timeout:
                pass
            except OSError:
                break

    @staticmethod
    def _echo_one(conn: socket.socket) -> None:
        with conn:
            conn.settimeout(0.8)
            try:
                data = conn.recv(32)
                if data:
                    conn.sendall(data)
            except OSError:
                pass

    def _heartbeat_loop(self) -> None:
        assert self.udp_sock is not None
        if psutil is not None:
            psutil.cpu_percent(interval=None)  # Inicializar lectura
        while not self.stop_event.is_set():
            begin = time.monotonic()
            cpu = psutil.cpu_percent(interval=None) if psutil is not None else 0.0
            # Trama exigida: PING:<node_id>|<cpu_usage>|<status>
            message = f"PING:{self.node_id}|{cpu:.1f}|READY"
            try:
                self.udp_sock.sendto(message.encode("utf-8"), (self.master_host, self.udp_port))
                data, _ = self.udp_sock.recvfrom(128)
                if data.decode("utf-8", errors="replace") != f"ACK:{self.node_id}":
                    self.logger.warning("ACK UDP inesperado")
            except socket.timeout:
                self.logger.warning("Sin ACK UDP (Master no responde o firewall)")
            except OSError as exc:
                if not self.stop_event.is_set():
                    self.logger.warning("Error enviando UDP: %s", exc)
            self.stop_event.wait(max(0, HEARTBEAT_INTERVAL - (time.monotonic() - begin)))

    def run_forever(self) -> None:
        """Reconecta el canal TCP si el Master se reinicia o cae la conexión."""
        if self.echo_sock is None:
            self.start()
        while not self.stop_event.is_set():
            try:
                with socket.create_connection((self.master_host, self.tcp_port), timeout=3) as sock:
                    sock.settimeout(None)
                    self.tcp_sock = sock
                    with sock.makefile("rb") as stream:
                        send_json(sock, {"type": "REGISTER", "node_id": self.node_id}, self.tcp_lock)
                        ack = recv_json(stream)
                        if ack is None or ack.get("type") != "REGISTER_ACK":
                            raise ValueError("Registro TCP no confirmado")
                        self.event(f"[TCP] Conectado al Master {self.master_host}:{self.tcp_port}")
                        while not self.stop_event.is_set():
                            message = recv_json(stream)
                            if message is None:
                                break
                            if message.get("type") == "JOB":
                                t = threading.Thread(target=self._run_job, args=(sock, message), daemon=True)
                                t.start()
                                self.background.append(t)
            except (OSError, ValueError, ConnectionError) as exc:
                if not self.stop_event.is_set():
                    self.logger.warning("TCP desconectado: %s", exc)
            finally:
                self.tcp_sock = None
            if not self.stop_event.is_set():
                self.event("Reintentando conexión TCP en 2 s...")
                self.stop_event.wait(2)

    def _run_job(self, sock: socket.socket, message: dict[str, Any]) -> None:
        job_id = str(message.get("job_id", "sin-id"))[:40]
        action = message.get("action")
        self.event(f"Orden TCP recibida: {action} (job={job_id})")
        try:
            if action == "SYSTEM_INFO":
                data = {"node_id": self.node_id, "os": platform.platform(),
                        "python": platform.python_version(), "cpu_count": psutil.cpu_count() if psutil else None}
            elif action == "FETCH_LOGS":
                with self.events_lock:
                    data = {"last_events": list(self.events)[-10:]}
            elif action == "SCAN_LOCAL_PORT":
                # Consulta únicamente el eco propio. NO escanea IP ni puertos externos.
                with socket.create_connection(("127.0.0.1", self.echo_port), timeout=1):
                    pass
                data = {"target": "127.0.0.1", "port": self.echo_port, "open": True}
            elif action == "STRESS_LOCAL":
                if not self.stress_lock.acquire(blocking=False):
                    data = {"error": "Ya hay una prueba activa en este Worker"}
                else:
                    try:
                        data = self._stress_local(message)
                    finally:
                        self.stress_lock.release()
            else:
                data = {"error": f"Acción desconocida: {action}"}
        except Exception as exc:
            data = {"error": f"Error en tarea {action}: {exc}"}
        try:
            send_json(sock, {"type": "RESULT", "node_id": self.node_id,
                             "job_id": job_id, "action": action, "data": data}, self.tcp_lock)
            self.event(f"Resultado {job_id} devuelto al Master")
        except OSError:
            self.logger.warning("No se pudo devolver el resultado %s: canal TCP cerrado", job_id)

    def _stress_local(self, message: dict[str, Any]) -> dict[str, Any]:
        """Simulación acotada en loopback del propio Worker (máx. 2 hilos)."""
        if message.get("target") != "127.0.0.1":
            raise ValueError("Solo se permite destino local 127.0.0.1")
        duration = message.get("duration")
        start_at = message.get("start_at")
        if type(duration) is not int or not 1 <= duration <= MAX_STRESS_SECONDS:
            raise ValueError("Duración fuera de rango: 1-10 segundos")
        if type(start_at) not in (int, float) or abs(start_at - time.time()) > 20:
            raise ValueError("Hora de inicio inválida o relojes sin sincronizar")
        self.event(f"Prueba preparada: {duration} s, hora de inicio UTC={start_at:.3f}")
        if self.stop_event.wait(max(0, start_at - time.time())):
            return {"error": "Worker detenido antes de la prueba"}
        started = time.time()
        deadline = time.monotonic() + duration

        def bounded_requests() -> tuple[list[float], int]:
            latency_ms: list[float] = []
            errors = 0
            while not self.stop_event.is_set() and time.monotonic() < deadline:
                begin = time.perf_counter()
                try:
                    with socket.create_connection(("127.0.0.1", self.echo_port), timeout=0.8) as local:
                        local.settimeout(0.8)
                        local.sendall(b"PING-LOCAL")
                        if local.recv(32) != b"PING-LOCAL":
                            raise OSError("Eco inesperado")
                    latency_ms.append((time.perf_counter() - begin) * 1000)
                except OSError:
                    errors += 1
                # Máximo ~8 solicitudes/s por Worker en total.
                self.stop_event.wait(0.25)
            return latency_ms, errors

        with ThreadPoolExecutor(max_workers=LOCAL_CONCURRENCY) as pool:
            futures = [pool.submit(bounded_requests) for _ in range(LOCAL_CONCURRENCY)]
            results = [future.result() for future in futures]
        times = sorted(t for measurements, _ in results for t in measurements)
        errors = sum(failed for _, failed in results)
        elapsed = max(time.time() - started, 0.001)
        return {"mode": "LOCAL_LOOPBACK_SAFE", "target": f"127.0.0.1:{self.echo_port}",
                "requested_start_utc": start_at, "actual_start_utc": started,
                "duration_s": round(elapsed, 3), "ok_requests": len(times),
                "errors": errors, "requests_per_second": round(len(times)/elapsed, 2),
                "avg_latency_ms": round(sum(times)/len(times), 3) if times else None,
                "p95_latency_ms": round(times[max(0, int(0.95*(len(times)-1)))], 3) if times else None,
                "concurrent_clients": LOCAL_CONCURRENCY}

    def stop(self) -> None:
        if self.stop_event.is_set():
            return
        self.stop_event.set()
        for sock in (self.udp_sock, self.echo_sock, self.tcp_sock):
            if sock:
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                try:
                    sock.close()
                except OSError:
                    pass
        for t in self.background:
            if t is not threading.current_thread() and t.is_alive():
                t.join(timeout=0.4)
        self.event("Worker detenido; recursos liberados")


def main() -> None:
    parser = argparse.ArgumentParser(description="Worker: latidos UDP + cliente TCP")
    parser.add_argument("--id", required=True, help="ID único (ej. worker1)")
    parser.add_argument("--master", default="127.0.0.1", help="IP del Master (Ethernet si aplica)")
    parser.add_argument("--tcp-port", type=int, default=5000)
    parser.add_argument("--udp-port", type=int, default=5001)
    args = parser.parse_args()
    worker = Worker(args.id, args.master, args.tcp_port, args.udp_port)
    try:
        worker.run_forever()
    except KeyboardInterrupt:
        print("\nCerrando Worker...")
    finally:
        worker.stop()


if __name__ == "__main__":
    main()
