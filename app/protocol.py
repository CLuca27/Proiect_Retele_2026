import json


MAX_MESSAGE_SIZE = 64 * 1024


def send_json(sock, message: dict):
    data = json.dumps(message).encode("utf-8") + b"\n"
    sock.sendall(data) 


def recv_json(sock) -> dict:
    data = b""

    while not data.endswith(b"\n"):
        chunk = sock.recv(4096)

        if not chunk:
            raise ConnectionError("Connection closed before full message was received")

        data += chunk

        if len(data) > MAX_MESSAGE_SIZE:
            raise ValueError("Message too large")

    return json.loads(data.decode("utf-8"))