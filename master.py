"""Nodo Master: heartbeats UDP + administración de Workers mediante TCP multihilo.

Uso: python master.py --host 0.0.0.0 --tcp-port 5000 --udp-port 5001
"""
from __future__ import annotations

import argparse
import logging
import socket
import threading
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from protocolo import recv_json, send_json, valid_node_id

HEARTBEAT_TIMEOUT = 6.0
MAX_STRESS_SECONDS = 10


def create_logger() -> logging.Logger:
    directory = Path(__file__).parent / "logs"
    directory.mkdir(exist_ok=True)
    logger = logging.getLogger("taller.master")
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s", "%H:%M:%S")
        for handler in (logging.StreamHandler(), logging.FileHandler(directory / "master.log", encoding="utf-8")):
            handler.setFormatter(formatter)
            logger.addHandler(handler)
    return logger


@dataclass
class Node:
    node_id: str
    last_seen: float = 0.0
    udp_addr: tuple[str, int] | None = None
    cpu: float = 0.0
    status: str = "UNKNOWN"
    alive: bool = False
    tcp_sock: socket.socket | None = None
    tcp_lock: threading.Lock = field(default_factory=threading.Lock)
    last_result: dict[str, Any] | None = None


class Master:
    def __init__(self, host: str = "0.0.0.0", tcp_port: int = 5000, udp_port: int = 5001):
        self.host = host
        self.tcp_port = tcp_port
        self.udp_port = udp_port
        self.logger = create_logger()
        self.nodes: dict[str, Node] = {}
        self.lock = threading.RLock()
        self.stop_event = threading.Event()
        self.tcp_server: socket.socket | None = None
        self.udp_server: socket.socket | None = None
        self.threads: list[threading.Thread] = []

    def start(self) -> None:
        self.tcp_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.tcp_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.tcp_server.bind((self.host, self.tcp_port))
        self.tcp_server.listen(32)
        self.tcp_server.settimeout(0.5)
        self.tcp_port = self.tcp_server.getsockname()[1]

        self.udp_server = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_server.bind((self.host, self.udp_port))
        self.udp_server.settimeout(0.5)
        self.udp_port = self.udp_server.getsockname()[1]

        for target, name in ((self._udp_loop, "UDP-heartbeats"),
                             (self._tcp_accept_loop, "TCP-accept"),
                             (self._monitor_loop, "Monitor-6s")):
            t = threading.Thread(target=target, name=name, daemon=True)
            self.threads.append(t)
            t.start()
        self.logger.info("Master iniciado | TCP=%s UDP=%s | host=%s", self.tcp_port, self.udp_port, self.host)

    def _udp_loop(self) -> None:
        assert self.udp_server is not None
        while not self.stop_event.is_set():
            try:
                data, addr = self.udp_server.recvfrom(1024)
                message = data.decode("utf-8").strip()
                # Trama exigida en el PDF: PING:<node_id>|<cpu_usage>|<status>
                if not message.startswith("PING:"):
                    continue
                node_id, cpu_txt, state = message[5:].split("|", 2)
                if not valid_node_id(node_id) or len(state) > 24:
                    continue
                cpu = float(cpu_txt)
                if not 0 <= cpu <= 100:
                    continue
                with self.lock:
                    node = self.nodes.setdefault(node_id, Node(node_id))
                    previously_alive = node.alive
                    node.last_seen = time.monotonic()
                    node.udp_addr = addr
                    node.cpu = cpu
                    node.status = state
                    node.alive = True
                self.udp_server.sendto(f"ACK:{node_id}".encode("utf-8"), addr)
                if not previously_alive:
                    self.logger.info("[UDP] Nodo REGISTRADO/RECUPERADO: %s desde %s", node_id, addr[0])
            except socket.timeout:
                pass
            except (UnicodeError, ValueError):
                self.logger.warning("Paquete UDP ignorado: formato inválido")
            except OSError:
                if not self.stop_event.is_set():
                    self.logger.exception("Error UDP")
                break

    def _monitor_loop(self) -> None:
        while not self.stop_event.wait(0.25):
            with self.lock:
                fallen = []
                now = time.monotonic()
                for node in self.nodes.values():
                    if node.alive and now - node.last_seen >= HEARTBEAT_TIMEOUT:
                        node.alive = False
                        fallen.append(node.node_id)
            for node_id in fallen:
                self.logger.warning("[UDP] %s CAÍDO/DESCONECTADO: 6 s sin latidos", node_id)

    def _tcp_accept_loop(self) -> None:
        assert self.tcp_server is not None
        while not self.stop_event.is_set():
            try:
                sock, addr = self.tcp_server.accept()
                t = threading.Thread(target=self._handle_worker, args=(sock, addr), daemon=True,
                                     name=f"TCP-{addr[0]}:{addr[1]}")
                t.start()
            except socket.timeout:
                continue
            except OSError:
                if not self.stop_event.is_set():
                    self.logger.exception("Error aceptando TCP")
                break

    def _handle_worker(self, sock: socket.socket, addr: tuple[str, int]) -> None:
        node_id: str | None = None
        try:
            sock.settimeout(8)  # Solo para el registro inicial
            with sock.makefile("rb") as stream:
                first = recv_json(stream)
                if first is None or first.get("type") != "REGISTER" or not valid_node_id(first.get("node_id")):
                    return
                node_id = first["node_id"]
                sock.settimeout(None)  # Sesión persistente sin tiempo máximo de lectura
                with self.lock:
                    node = self.nodes.setdefault(node_id, Node(node_id))
                    previous = node.tcp_sock
                    node.tcp_sock = sock
                    node.tcp_lock = threading.Lock()
                if previous is not None and previous is not sock:
                    try:
                        previous.shutdown(socket.SHUT_RDWR)
                    except OSError:
                        pass
                    previous.close()
                send_json(sock, {"type": "REGISTER_ACK", "node_id": node_id})
                self.logger.info("[TCP] %s conectado desde %s:%s", node_id, *addr)

                while not self.stop_event.is_set():
                    message = recv_json(stream)
                    if message is None:
                        break
                    if message.get("type") == "RESULT":
                        # Evitar que un socket reemplazado escriba sobre el nuevo.
                        with self.lock:
                            if node.tcp_sock is sock:
                                node.last_result = message
                        self.logger.info("[RESULTADO] %s | %s | %s", node_id,
                                         message.get("job_id", "?"), message.get("data", {}))
        except (OSError, ConnectionError, ValueError, UnicodeError) as exc:
            if not self.stop_event.is_set():
                self.logger.warning("[TCP] Conexión de %s terminada: %s", node_id or addr, exc)
        finally:
            with self.lock:
                if node_id and node_id in self.nodes and self.nodes[node_id].tcp_sock is sock:
                    self.nodes[node_id].tcp_sock = None
            try:
                sock.close()
            except OSError:
                pass
            if node_id:
                self.logger.info("[TCP] %s desconectado (estado UDP independiente)", node_id)

    def send_job(self, node_id: str, job: dict[str, Any]) -> bool:
        with self.lock:
            node = self.nodes.get(node_id)
            if node is None or node.tcp_sock is None or not node.alive:
                return False
            sock, lock = node.tcp_sock, node.tcp_lock
        try:
            send_json(sock, job, lock)
            return True
        except OSError as exc:
            self.logger.warning("Envío a %s falló: %s", node_id, exc)
            return False

    def active_nodes(self) -> list[str]:
        with self.lock:
            return [key for key, node in self.nodes.items() if node.alive and node.tcp_sock is not None]

    def table(self) -> str:
        lines = [f"{'NODO':<18} {'UDP':<12} {'TCP':<9} {'CPU':<8} {'ÚLTIMO PING'}"]
        with self.lock:
            for key, node in sorted(self.nodes.items()):
                age = f"{time.monotonic()-node.last_seen:.1f} s" if node.last_seen else "Nunca"
                lines.append(f"{key:<18} {('ACTIVO' if node.alive else 'CAÍDO'):<12} "
                             f"{('SÍ' if node.tcp_sock else 'NO'):<9} {node.cpu:>5.1f}%  {age}")
        return "\n".join(lines) + ("" if len(lines) > 1 else "\n(Aún no hay Workers)")

    def broadcast_stress(self, seconds: int) -> int:
        if not 1 <= seconds <= MAX_STRESS_SECONDS:
            raise ValueError(f"Duración permitida: 1-{MAX_STRESS_SECONDS} s")
        # Solo carga sintética local en 127.0.0.1 de CADA Worker. Nunca IP externas.
        job = {"type": "JOB", "job_id": uuid.uuid4().hex[:8], "action": "STRESS_LOCAL",
               "duration": seconds, "start_at": time.time() + 3.0, "target": "127.0.0.1"}
        targets = self.active_nodes()
        sent = sum(self.send_job(node_id, job) for node_id in targets)
        self.logger.info("[BROADCAST] Trabajo %s enviado a %s/%s Workers; inicio aprox. en 3 s",
                         job["job_id"], sent, len(targets))
        return sent

    def command(self, raw: str) -> bool:
        """Devuelve False si el operador quiere salir."""
        parts = raw.strip().split()
        if not parts:
            return True
        action = parts[0].lower()
        if action in {"salir", "exit", "quit"}:
            return False
        if action in {"ayuda", "help"}:
            print("\nCOMANDOS:\n  nodos\n  info <id>\n  scan <id>\n  logs <id>\n"
                  "  stress <segundos> (1-10, prueba local autorizada)\n"
                  "  BROADCAST_STRESS:127.0.0.1:<segundos>\n  salir\n")
        elif action == "nodos":
            print(self.table())
        elif action in {"info", "scan", "logs"} and len(parts) == 2:
            task = {"info": "SYSTEM_INFO", "scan": "SCAN_LOCAL_PORT", "logs": "FETCH_LOGS"}[action]
            job = {"type": "JOB", "job_id": uuid.uuid4().hex[:8], "action": task}
            print("Comando enviado" if self.send_job(parts[1], job)
                  else "Nodo no encontrado, sin latido o sin conexión TCP")
        elif action == "stress" and len(parts) == 2:
            try:
                print(f"Orden global enviada a {self.broadcast_stress(int(parts[1]))} Workers")
            except ValueError as exc:
                print(f"Error: {exc}")
        elif raw.strip().upper().startswith("BROADCAST_STRESS:"):
            try:
                label, target, duration_txt = raw.strip().split(":")
                if target != "127.0.0.1":
                    print("Solo se permite 127.0.0.1 (loopback de cada Worker)")
                else:
                    print(f"Orden global enviada a {self.broadcast_stress(int(duration_txt))} Workers")
            except ValueError:
                print("Uso: BROADCAST_STRESS:127.0.0.1:3")
        else:
            print("Comando desconocido. Escribe 'ayuda'.")
        return True

    def stop(self) -> None:
        if self.stop_event.is_set():
            return
        self.stop_event.set()
        for sock in (self.tcp_server, self.udp_server):
            if sock:
                try:
                    sock.close()
                except OSError:
                    pass
        with self.lock:
            peers = [n.tcp_sock for n in self.nodes.values() if n.tcp_sock is not None]
        for peer in peers:
            try:
                peer.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            peer.close()
        for t in self.threads:
            t.join(timeout=1)
        self.logger.info("Master detenido, sockets cerrados")


def main() -> None:
    parser = argparse.ArgumentParser(description="Master: UDP heartbeat + TCP multihilo")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--tcp-port", type=int, default=5000)
    parser.add_argument("--udp-port", type=int, default=5001)
    args = parser.parse_args()
    master = Master(args.host, args.tcp_port, args.udp_port)
    try:
        master.start()
        print("Escribe 'ayuda' para ver los comandos.")
        while master.command(input("master> ")):
            pass
    except (KeyboardInterrupt, EOFError):
        print("\nApagando...")
    finally:
        master.stop()


if __name__ == "__main__":
    main()
