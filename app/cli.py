import argparse
import base64
import json
import socket

from app.protocol import recv_json, send_json


def send_local_command(host: str, port: int, command: dict) -> dict:
    with socket.create_connection((host, port), timeout=5) as sock:
        send_json(sock, command)
        return recv_json(sock)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5000)

    subparsers = parser.add_subparsers(dest="action", required=True)

    subscribe_parser = subparsers.add_parser("subscribe")
    subscribe_parser.add_argument("key")

    unsubscribe_parser = subparsers.add_parser("unsubscribe")
    unsubscribe_parser.add_argument("key")

    publish_parser = subparsers.add_parser("publish")
    publish_parser.add_argument("key")
    publish_parser.add_argument("payload", nargs="+")

    subparsers.add_parser("subs")
    subparsers.add_parser("peers")
    subparsers.add_parser("queue")

    args = parser.parse_args()

    command = {
        "type": "LOCAL_COMMAND",
        "action": args.action,
    }

    if args.action in ("subscribe", "unsubscribe"):
        command["key"] = args.key

    elif args.action == "publish":
        payload_text = " ".join(args.payload)
        command["key"] = args.key
        command["payload_b64"] = base64.b64encode(
            payload_text.encode("utf-8")
        ).decode("ascii")

    response = send_local_command(args.host, args.port, command)
    print(json.dumps(response, indent=2))


if __name__ == "__main__":
    main()
