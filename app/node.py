import socket
import threading
import time
import uuid
from dataclasses import dataclass
from app.protocol import recv_json, send_json


@dataclass
class NodeConfig:
    node_id: str
    host: str
    port: int
    callback_host: str
    callback_port: int
    upstreams: list[tuple[str, int]]


class Node:
    def __init__(self, config: NodeConfig):
        self.config = config
        self.running = True
        self.active_upstream = None
        self.known_nodes = {}
        self.child_nodes = set()
        self.seen_events = set()

    def start(self):
        server_thread = threading.Thread(target=self.start_server, daemon=True)
        server_thread.start()

        time.sleep(0.3)
        self.connect_to_first_available_upstream()

        print(f"[{self.config.node_id}] Node started. Press Ctrl+C to stop.")

        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            print(f"\n[{self.config.node_id}] Stopping node...")
            self.running = False

    def start_server(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server_socket.bind((self.config.host, self.config.port))
            server_socket.listen()

            print(
                f"[{self.config.node_id}] Server listening on "
                f"{self.config.host}:{self.config.port}"
            )

            while self.running:
                client_socket, client_address = server_socket.accept()

                client_thread = threading.Thread(
                    target=self.handle_client,
                    args=(client_socket, client_address),
                    daemon=True,
                )
                client_thread.start()

    def handle_client(self, client_socket, client_address):
        with client_socket:
            try:
                message = recv_json(client_socket)
                message_type = message.get("type")

                if message_type == "HELLO":
                    node_id = message["node_id"]
                    callback_host = message["callback_host"]
                    callback_port = message["callback_port"]

                    self.known_nodes[node_id] = (callback_host, callback_port)
                    self.child_nodes.add(node_id)

                    event_id = str(uuid.uuid4())
                    self.seen_events.add(event_id)

                    self.propagate_event(
                        {
                            "type": "PEER_JOIN",
                            "event_id": event_id,
                            "node_id": node_id,
                            "callback_host": callback_host,
                            "callback_port": callback_port,
                        },
                        exclude_node_id=node_id,
                    )

                    print(
                        f"[{self.config.node_id}] HELLO from {node_id} | "
                        f"callback={callback_host}:{callback_port}"
                    )

                    print(f"[{self.config.node_id}] Known nodes: {self.known_nodes}")

                    send_json(
                        client_socket,
                        {
                            "type": "HELLO_ACK",
                            "node_id": self.config.node_id,
                            "status": "OK",
                        },
                    )

                elif message_type == "PEER_JOIN":
                    self.handle_peer_join(message)

                    send_json(
                        client_socket,
                        {
                            "type": "PEER_JOIN_ACK",
                            "node_id": self.config.node_id,
                            "status": "OK",
                        },
                    )

                else:
                    send_json(
                        client_socket,
                        {
                            "type": "ERROR",
                            "error": f"Unknown message type: {message_type}",
                        },
                    )

            except Exception as error:
                print(f"[{self.config.node_id}] Client error: {error}")

    def connect_to_first_available_upstream(self):
        if not self.config.upstreams:
            print(f"[{self.config.node_id}] No upstream configured.")
            return

        for host, port in self.config.upstreams:
            print(f"[{self.config.node_id}] Trying upstream {host}:{port}...")

            try:
                with socket.create_connection((host, port), timeout=3) as sock:
                    send_json(
                        sock,
                        {
                            "type": "HELLO",
                            "node_id": self.config.node_id,
                            "callback_host": self.config.callback_host,
                            "callback_port": self.config.callback_port,
                        },
                    )

                    response = recv_json(sock)
                    upstream_node_id = response.get("node_id")

                    if upstream_node_id:
                        self.known_nodes[upstream_node_id] = (host, port)

                    self.active_upstream = (host, port)

                    print(
                        f"[{self.config.node_id}] Connected to upstream "
                        f"{host}:{port} | response={response}"
                    )

                    return

            except OSError as error:
                print(
                    f"[{self.config.node_id}] Failed to connect to "
                    f"{host}:{port} | error={error}"
                )

        print(f"[{self.config.node_id}] No upstream available.")

    def handle_peer_join(self, message: dict):
        event_id = message["event_id"]

        if event_id in self.seen_events:
            print(f"[{self.config.node_id}] Ignored duplicate PEER_JOIN {event_id}")
            return

        self.seen_events.add(event_id)

        node_id = message["node_id"]
        callback_host = message["callback_host"]
        callback_port = message["callback_port"]

        if node_id == self.config.node_id:
            return

        self.known_nodes[node_id] = (callback_host, callback_port)

        print(
            f"[{self.config.node_id}] Learned peer {node_id} "
            f"at {callback_host}:{callback_port}"
        )

        print(f"[{self.config.node_id}] Known nodes: {self.known_nodes}")

        self.propagate_event(message, exclude_node_id=node_id)

    def send_event_to_node(self, target_node_id: str, event: dict):
        if target_node_id not in self.known_nodes:
            return

        host, port = self.known_nodes[target_node_id]

        try:
            with socket.create_connection((host, port), timeout=3) as sock:
                send_json(sock, event)
                response = recv_json(sock)

                print(
                    f"[{self.config.node_id}] Sent {event['type']} to "
                    f"{target_node_id} | response={response}"
                )

        except OSError as error:
            print(
                f"[{self.config.node_id}] Failed to send {event['type']} to "
                f"{target_node_id} | error={error}"
            )

    def propagate_event(self, event: dict, exclude_node_id: str | None = None):
        targets = set(self.child_nodes)

        if self.active_upstream:
            upstream_host, upstream_port = self.active_upstream

            for node_id, address in self.known_nodes.items():
                if address == (upstream_host, upstream_port):
                    targets.add(node_id)

        targets.discard(self.config.node_id)

        if exclude_node_id:
            targets.discard(exclude_node_id)

        for target_node_id in targets:
            self.send_event_to_node(target_node_id, event)


            

