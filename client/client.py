import asyncio
import sys
import logging
from client.ui import ChatUI
from shared.utils import setup_logger
from shared.protocol import send_packet, receive_packet, PacketType

class ChatClient:
    def __init__(self):
        self.logger = setup_logger("Client")
        self.reader = None
        self.writer = None
        self.host = "127.0.0.1"
        self.port = 8000

    async def connect(self):
        """
        Establishes a TCP connection to the Leader.
        """
        self.logger.info(f"Connecting to Leader at {self.host}:{self.port}...")
        try:
            self.reader, self.writer = await asyncio.open_connection(self.host, self.port)
            self.logger.info("Connected successfully.")
            return True
        except Exception as e:
            self.logger.error(f"Failed to connect: {e}")
            return False

    async def disconnect(self):
        """
        Gracefully disconnects from the Leader.
        """
        if self.writer:
            self.logger.info("Disconnecting from Leader...")
            self.writer.close()
            await self.writer.wait_closed()
            self.writer = None
            self.reader = None
            self.logger.info("Disconnected.")

async def main():
    client = ChatClient()
    client.logger.info("Starting ResiliChat Client")

    ui = ChatUI()
    ui.display_banner()

    try:
        await ui.main_menu()
    except KeyboardInterrupt:
        print("\nExiting...")
    finally:
        await client.disconnect()
        client.logger.info("Client shut down")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(0)
