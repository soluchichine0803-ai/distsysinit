import argparse
import asyncio
import sys
from server.server import Server

async def main():
    parser = argparse.ArgumentParser(description="Start Chat Server as Leader")
    parser.add_argument("--server-id", type=int, required=True, help="Unique ID for this server")
    args = parser.parse_args()

    leader = Server(server_id=args.server_id, is_leader=True)

    try:
        await leader.start()
        # Keep running until interrupted
        while True:
            await asyncio.sleep(3600)
    except KeyboardInterrupt:
        pass
    finally:
        await leader.stop()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(0)
