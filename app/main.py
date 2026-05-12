import argparse

from app.node import Node, NodeConfig


def parse_upstreams(value: str) -> list[tuple[str, int]]:
    if not value:
        return []

    upstreams = []

    for item in value.split(","):
        host, port = item.split(":")
        upstreams.append((host, int(port)))

    return upstreams


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--node-id", required=True)
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--upstreams", default="") 
    parser.add_argument("--callback-host", default="127.0.0.1")
    parser.add_argument("--callback-port", type=int)


    args = parser.parse_args() 

    callback_port = args.callback_port or args.port

    config = NodeConfig(
        node_id=args.node_id,
        host=args.host,
        port=args.port, 
        callback_host=args.callback_host, 
        callback_port=callback_port,
        upstreams=parse_upstreams(args.upstreams)    
        )

    node = Node(config)
    node.start()


if __name__ == "__main__":
    main()