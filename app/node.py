import socket
import threading
import time
import uuid
import queue
import base64
import errno
from dataclasses import dataclass
from app.protocol import recv_json, send_json
from app.commands import process_message

@dataclass
class NodeConfig:
    node_id: str
    host: str
    port: int
    callback_host: str
    callback_port: int
    upstreams: list[tuple[str, int]]
    port_search_limit: int = 20


class Node:
    def __init__(self, config: NodeConfig):
        self.config = config
        self.running = True
        self.active_upstream = None
        self.known_nodes = {}
        self.child_nodes = set()
        self.seen_events = set()
        self.subscriptions = {}
        self.message_queue = queue.Queue()
        self.max_payload_size = 1024 * 1024
        self.delivery_results = {}
        self.delivery_result_events = {}
        self.delivery_results_lock = threading.Lock()

    def start(self):
        try:
            server_socket = self.create_server_socket()
        except OSError as error:
            print(f"[{self.config.node_id}] Failed to start server: {error}")
            self.running = False
            return

        server_thread = threading.Thread(
            target=self.start_server,
            args=(server_socket,),
            daemon=True,
        )
        server_thread.start()

        console_thread = threading.Thread(target=self.console_loop, daemon=True)
        console_thread.start()

        dispatcher_thread = threading.Thread(target=self.dispatch_loop, daemon=True)
        dispatcher_thread.start()

        time.sleep(0.3)
        self.connect_to_first_available_upstream()

        print(f"[{self.config.node_id}] Node started. Press Ctrl+C to stop.")

        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            print(f"\n[{self.config.node_id}] Stopping node...")
            self.running = False

    def create_server_socket(self):
        last_error = None
        first_port = self.config.port
        last_port = self.config.port + self.config.port_search_limit

        for port in range(first_port, last_port + 1):
            server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

            try:
                server_socket.bind((self.config.host, port))
                server_socket.listen()

                if port != self.config.port:
                    print(
                        f"[{self.config.node_id}] Port {self.config.port} is busy. "
                        f"Using port {port} instead."
                    )
                    self.config.port = port
                    self.config.callback_port = port

                print(
                    f"[{self.config.node_id}] Server listening on "
                    f"{self.config.host}:{self.config.port}"
                )

                return server_socket

            except OSError as error:
                server_socket.close()
                last_error = error

                if not self.is_address_in_use(error):
                    raise

                print(
                    f"[{self.config.node_id}] Port {port} is unavailable, "
                    "trying next port..."
                )

        raise OSError(
            f"No free port found in range {first_port}-{last_port}"
        ) from last_error

    def is_address_in_use(self, error: OSError) -> bool:
        return (
            error.errno == errno.EADDRINUSE
            or error.errno == errno.EACCES
            or getattr(error, "winerror", None) == 10048
            or getattr(error, "winerror", None) == 10013
        )

    def start_server(self, server_socket):
        with server_socket:
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

                elif message_type == "SUBSCRIBE":
                    self.handle_subscribe_event(message)

                    send_json(
                        client_socket,
                        {
                            "type": "SUBSCRIBE_ACK",
                            "node_id": self.config.node_id,
                            "status": "OK",
                        },
                    )

                elif message_type == "UNSUBSCRIBE":
                    self.handle_unsubscribe_event(message)

                    send_json(
                        client_socket,
                        {
                            "type": "UNSUBSCRIBE_ACK",
                            "node_id": self.config.node_id,
                            "status": "OK",
                        },
                    )

                elif message_type == "PEER_LEFT":
                    self.handle_peer_left(message)

                    send_json(
                        client_socket,
                        {
                            "type": "PEER_LEFT_ACK",
                            "node_id": self.config.node_id,
                            "status": "OK",
                        },
                    )

                elif message_type == "PUBLISH":
                    response = self.handle_publish_event(message)
                    send_json(client_socket, response)

                elif message_type == "DELIVER":
                    ack = self.handle_deliver_event(message)

                    response = {
                        "type": "ACK",
                        "node_id": self.config.node_id,
                        "message_id": message.get("message_id"),
                        "status": ack["status"],
                    }

                    if "result" in ack:
                        response["result"] = ack["result"]

                    if "error" in ack:
                        response["error"] = ack["error"]

                    send_json(client_socket, response)

                elif message_type == "LOCAL_COMMAND":
                    response = self.handle_local_command(message)
                    send_json(client_socket, response)

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
                event_to_send = dict(event)
                event_to_send["sender_node_id"] = self.config.node_id

                send_json(sock, event_to_send)
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

        sender_node_id = event.get("sender_node_id")
        if sender_node_id:
            targets.discard(sender_node_id)

        if exclude_node_id:
            targets.discard(exclude_node_id)

        for target_node_id in targets:
            self.send_event_to_node(target_node_id, event)

    def console_loop(self):
        while self.running:
            try:
                command = input().strip()
            except EOFError:
                return

            if not command:
                continue

            parts = command.split(maxsplit=1)
            action = parts[0].lower()

            if action in ("subscribe", "sub"):
                if len(parts) < 2:
                    print(f"[{self.config.node_id}] Usage: subscribe <key>")
                    continue

                self.subscribe(parts[1])

            elif action in ("unsubscribe", "unsub"):
                if len(parts) < 2:
                    print(f"[{self.config.node_id}] Usage: unsubscribe <key>")
                    continue

                self.unsubscribe(parts[1])

            elif action == "publish":
                if len(parts) < 2:
                    print(f"[{self.config.node_id}] Usage: publish <key> <payload>")
                    continue

                publish_parts = parts[1].split(maxsplit=1)

                if len(publish_parts) < 2:
                    print(f"[{self.config.node_id}] Usage: publish <key> <payload>")
                    continue

                key, payload_text = publish_parts
                self.publish(key, payload_text.encode("utf-8"))

            elif action == "queue":
                print(f"[{self.config.node_id}] Queue size: {self.message_queue.qsize()}")

            elif action == "subs":
                print(f"[{self.config.node_id}] Subscriptions: {self.subscriptions}")

            elif action == "peers":
                print(f"[{self.config.node_id}] Known nodes: {self.known_nodes}")

            elif action == "exit":
                self.running = False
                return

            else:
                print(f"[{self.config.node_id}] Unknown command: {command}")

    def handle_local_command(self, message: dict) -> dict:
        action = message.get("action")

        try:
            if action == "subscribe":
                key = message["key"]
                self.subscribe(key)
                return {
                    "type": "LOCAL_COMMAND_ACK",
                    "status": "OK",
                    "action": action,
                    "key": key,
                }

            if action == "unsubscribe":
                key = message["key"]
                self.unsubscribe(key)
                return {
                    "type": "LOCAL_COMMAND_ACK",
                    "status": "OK",
                    "action": action,
                    "key": key,
                }

            if action == "publish":
                key = message["key"]
                payload = base64.b64decode(message["payload_b64"].encode("ascii"))
                publish_ack = self.publish(key, payload, track_results=False)
                return {
                    "type": "LOCAL_COMMAND_ACK",
                    "status": publish_ack["status"],
                    "action": action,
                    "key": key,
                    **{
                        field: publish_ack[field]
                        for field in ("message_id", "size", "error")
                        if field in publish_ack
                    },
                }

            if action == "subs":
                return {
                    "type": "LOCAL_COMMAND_ACK",
                    "status": "OK",
                    "subscriptions": self.subscriptions_snapshot(),
                }

            if action == "peers":
                return {
                    "type": "LOCAL_COMMAND_ACK",
                    "status": "OK",
                    "known_nodes": self.known_nodes,
                }

            if action == "queue":
                return {
                    "type": "LOCAL_COMMAND_ACK",
                    "status": "OK",
                    "queue_size": self.message_queue.qsize(),
                }

            return {
                "type": "LOCAL_COMMAND_ACK",
                "status": "ERROR",
                "error": f"Unknown local action: {action}",
            }

        except Exception as error:
            return {
                "type": "LOCAL_COMMAND_ACK",
                "status": "ERROR",
                "action": action,
                "error": str(error),
            }

    def subscriptions_snapshot(self) -> dict:
        return {
            key: sorted(subscribers)
            for key, subscribers in self.subscriptions.items()
        }

    def subscribe(self, key: str):
        event_id = str(uuid.uuid4())
        self.seen_events.add(event_id)

        self.add_subscription(key, self.config.node_id)

        event = {
            "type": "SUBSCRIBE",
            "event_id": event_id,
            "node_id": self.config.node_id,
            "key": key,
            "callback_host": self.config.callback_host,
            "callback_port": self.config.callback_port,
        }

        print(f"[{self.config.node_id}] Subscribed to key={key}")
        print(f"[{self.config.node_id}] Subscriptions: {self.subscriptions}")

        self.propagate_event(event, exclude_node_id=self.config.node_id)

    def unsubscribe(self, key: str):
        event_id = str(uuid.uuid4())
        self.seen_events.add(event_id)

        self.remove_subscription(key, self.config.node_id)

        event = {
            "type": "UNSUBSCRIBE",
            "event_id": event_id,
            "node_id": self.config.node_id,
            "key": key,
        }

        print(f"[{self.config.node_id}] Unsubscribed from key={key}")
        print(f"[{self.config.node_id}] Subscriptions: {self.subscriptions}")

        self.propagate_event(event, exclude_node_id=self.config.node_id)

    def add_subscription(self, key: str, node_id: str):
        if key not in self.subscriptions:
            self.subscriptions[key] = set()

        self.subscriptions[key].add(node_id)

    def remove_subscription(self, key: str, node_id: str):
        if key not in self.subscriptions:
            return

        self.subscriptions[key].discard(node_id)

        if not self.subscriptions[key]:
            del self.subscriptions[key]

    def handle_subscribe_event(self, message: dict):
        event_id = message["event_id"]

        if event_id in self.seen_events:
            print(f"[{self.config.node_id}] Ignored duplicate SUBSCRIBE {event_id}")
            return

        self.seen_events.add(event_id)

        node_id = message["node_id"]
        key = message["key"]

        callback_host = message.get("callback_host")
        callback_port = message.get("callback_port")

        if callback_host and callback_port:
            self.known_nodes[node_id] = (callback_host, callback_port)

        self.add_subscription(key, node_id)

        print(f"[{self.config.node_id}] Learned subscription: {key} -> {node_id}")
        print(f"[{self.config.node_id}] Subscriptions: {self.subscriptions}")

        self.propagate_event(message, exclude_node_id=node_id)

    def handle_unsubscribe_event(self, message: dict):
        event_id = message["event_id"]

        if event_id in self.seen_events:
            print(f"[{self.config.node_id}] Ignored duplicate UNSUBSCRIBE {event_id}")
            return

        self.seen_events.add(event_id)

        node_id = message["node_id"]
        key = message["key"]

        self.remove_subscription(key, node_id)

        print(f"[{self.config.node_id}] Learned unsubscribe: {key} -> {node_id}")
        print(f"[{self.config.node_id}] Subscriptions: {self.subscriptions}")

        self.propagate_event(message, exclude_node_id=node_id)

    def handle_peer_left(self, message: dict):
        event_id = message["event_id"]

        if event_id in self.seen_events:
            print(f"[{self.config.node_id}] Ignored duplicate PEER_LEFT {event_id}")
            return

        self.seen_events.add(event_id)

        node_id = message["node_id"]

        if node_id == self.config.node_id:
            return

        self.remove_node(node_id)
        self.propagate_event(message, exclude_node_id=node_id)

    def handle_publish_event(self, message: dict) -> dict:
        try:
            key = message.get("key", "")
            payload_b64 = message["payload_b64"]
            payload = base64.b64decode(payload_b64.encode("ascii"))
            wait_for_results = bool(message.get("wait_for_results", True))
            result_timeout = float(message.get("result_timeout", 5))
            publish_ack = self.publish(
                key,
                payload,
                track_results=wait_for_results,
            )

            response = {
                "type": "PUBLISH_ACK",
                "node_id": self.config.node_id,
                "status": publish_ack["status"],
                "key": key,
            }

            for field in ("message_id", "size", "error"):
                if field in publish_ack:
                    response[field] = publish_ack[field]

            if publish_ack["status"] == "OK" and wait_for_results:
                completed, delivery_results = self.wait_for_delivery_results(
                    publish_ack["message_id"],
                    result_timeout,
                )
                response["delivery_completed"] = completed
                response["delivery_results"] = delivery_results

            return response

        except Exception as error:
            return {
                "type": "PUBLISH_ACK",
                "node_id": self.config.node_id,
                "status": "ERROR",
                "error": str(error),
            }

    def publish(self, key: str, payload: bytes, track_results: bool = False):
        if not key:
            print(f"[{self.config.node_id}] Publish failed: missing key")
            return {
                "status": "ERROR",
                "error": "missing key",
            }

        if len(payload) > self.max_payload_size:
            print(f"[{self.config.node_id}] Publish failed: payload too large")
            return {
                "status": "ERROR",
                "error": "payload too large",
            }

        message = {
            "message_id": str(uuid.uuid4()),
            "key": key,
            "payload": payload,
            "producer_id": self.config.node_id,
        }

        if track_results:
            self.register_delivery_tracking(message["message_id"])

        self.message_queue.put(message)

        print(
            f"[{self.config.node_id}] Queued message "
            f"{message['message_id']} key={key} size={len(payload)} bytes"
        )

        return {
            "status": "OK",
            "message_id": message["message_id"],
            "size": len(payload),
        }

    def register_delivery_tracking(self, message_id: str):
        with self.delivery_results_lock:
            self.delivery_results[message_id] = []
            self.delivery_result_events[message_id] = threading.Event()

    def store_delivery_results(self, message_id: str, results: list[dict]):
        with self.delivery_results_lock:
            if (
                message_id not in self.delivery_results
                and message_id not in self.delivery_result_events
            ):
                return

            self.delivery_results[message_id] = results
            event = self.delivery_result_events.get(message_id)

        if event:
            event.set()

    def wait_for_delivery_results(
        self,
        message_id: str,
        timeout: float,
    ) -> tuple[bool, list[dict]]:
        with self.delivery_results_lock:
            event = self.delivery_result_events.get(message_id)

        if not event:
            return True, []

        completed = event.wait(timeout)

        with self.delivery_results_lock:
            results = list(self.delivery_results.get(message_id, []))
            self.delivery_result_events.pop(message_id, None)
            self.delivery_results.pop(message_id, None)

        return completed, results

    def dispatch_loop(self):
        while self.running:
            try:
                message = self.message_queue.get(timeout=1)
            except queue.Empty:
                continue

            self.dispatch_message(message)
            self.message_queue.task_done()

    def dispatch_message(self, message: dict):
        key = message["key"]
        subscribers = set(self.subscriptions.get(key, set()))
        delivery_results = []

        if not subscribers:
            print(
                f"[{self.config.node_id}] No subscribers for "
                f"message {message['message_id']} key={key}"
            )
            self.store_delivery_results(message["message_id"], delivery_results)
            return

        print(
            f"[{self.config.node_id}] Dispatching message "
            f"{message['message_id']} key={key} to {subscribers}"
        )

        for subscriber_id in subscribers:
            if subscriber_id == self.config.node_id:
                ack = self.handle_local_delivery(message)
                delivery_results.append({
                    "node_id": self.config.node_id,
                    **ack,
                })

                print(
                    f"[{self.config.node_id}] Local ACK "
                    f"message_id={message['message_id']} status={ack['status']}"
                )
            else:
                ack = self.deliver_message_to_node(subscriber_id, message)
                delivery_results.append({
                    "node_id": subscriber_id,
                    **ack,
                })

        self.store_delivery_results(message["message_id"], delivery_results)

    def deliver_message_to_node(self, target_node_id: str, message: dict):
        if target_node_id not in self.known_nodes:
            print(
                f"[{self.config.node_id}] Cannot deliver "
                f"{message['message_id']} to {target_node_id}: unknown node"
            )
            return {
                "status": "ERROR",
                "error": "unknown node",
            }

        host, port = self.known_nodes[target_node_id]

        deliver_event = {
            "type": "DELIVER",
            "message_id": message["message_id"],
            "key": message["key"],
            "payload_b64": base64.b64encode(message["payload"]).decode("ascii"),
            "producer_id": message["producer_id"],
            "broker_id": self.config.node_id,
        }

        try:
            with socket.create_connection((host, port), timeout=3) as sock:
                send_json(sock, deliver_event)
                response = recv_json(sock)

                print(
                    f"[{self.config.node_id}] Delivered {message['message_id']} "
                    f"to {target_node_id} | response={response}"
                )

                return response

        except Exception as error:
            print(
                f"[{self.config.node_id}] Delivery failed for "
                f"{message['message_id']} to {target_node_id} | error={error}"
            )
            self.remove_node(target_node_id)
            self.announce_peer_left(target_node_id)
            return {
                "status": "ERROR",
                "error": str(error),
            }

    def handle_local_delivery(self, message: dict) -> dict:
        print(
            f"[{self.config.node_id}] Processing message "
            f"{message['message_id']} key={message['key']}"
        )

        try:
            result = process_message(message["key"], message["payload"])

            print(
                f"[{self.config.node_id}] Processed message "
                f"{message['message_id']} result={result}"
            )

            return {
                "status": "OK",
                "result": result,
            }

        except Exception as error:
            print(
                f"[{self.config.node_id}] Processing failed for "
                f"{message['message_id']} | error={error}"
            )

            return {
                "status": "ERROR",
                "error": str(error),
            }

    def handle_deliver_event(self, message: dict) -> dict:
        payload = base64.b64decode(message["payload_b64"].encode("ascii"))

        local_message = {
            "message_id": message["message_id"],
            "key": message["key"],
            "payload": payload,
            "producer_id": message["producer_id"],
            "broker_id": message["broker_id"],
        }

        return self.handle_local_delivery(local_message)

    def remove_node(self, node_id: str):
        if node_id in self.known_nodes:
            del self.known_nodes[node_id]

        self.child_nodes.discard(node_id)

        empty_keys = []

        for key, subscribers in self.subscriptions.items():
            subscribers.discard(node_id)

            if not subscribers:
                empty_keys.append(key)

        for key in empty_keys:
            del self.subscriptions[key]

        print(f"[{self.config.node_id}] Removed node {node_id}")
        print(f"[{self.config.node_id}] Known nodes: {self.known_nodes}")
        print(f"[{self.config.node_id}] Subscriptions: {self.subscriptions}")

    def announce_peer_left(self, node_id: str):
        event_id = str(uuid.uuid4())
        self.seen_events.add(event_id)

        event = {
            "type": "PEER_LEFT",
            "event_id": event_id,
            "node_id": node_id,
        }
        
        for target_node_id in list(self.known_nodes.keys()):
            if target_node_id != node_id:
                self.send_event_to_node(target_node_id, event)

