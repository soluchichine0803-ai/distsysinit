import asyncio
import sys
import logging
import json
import os
from client.ui import ChatUI
from shared.utils import setup_logger
from shared.protocol import send_packet, receive_packet, PacketType
from shared.models import Message

class ChatClient:
    def __init__(self):
        self.logger = setup_logger("Client")
        self.reader = None
        self.writer = None
        self.host = "127.0.0.1"
        self.port = 8000
        self.config_file = ".client_config"
        self.username = None
        self.ui = None
        self.load_config()

    def load_config(self):
        if os.path.exists(self.config_file):
            try:
                with open(self.config_file, 'r') as f:
                    config = json.load(f)
                    self.host = config.get("server_ip", "127.0.0.1")
                    self.port = config.get("server_port", 8000)
                self.logger.info(f"Loaded config: {self.host}:{self.port}")
            except Exception as e:
                self.logger.error(f"Failed to load config: {e}")

    def save_config(self):
        try:
            with open(self.config_file, 'w') as f:
                json.dump({"server_ip": self.host, "server_port": self.port}, f)
            self.logger.info(f"Saved config: {self.host}:{self.port}")
        except Exception as e:
            self.logger.error(f"Failed to save config: {e}")

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
            try:
                self.writer.close()
                await self.writer.wait_closed()
            except:
                pass
            self.writer = None
            self.reader = None
            self.logger.info("Disconnected.")

    async def register(self, username, password):
        if not await self.connect():
            return False, "Could not connect to server"

        await send_packet(self.writer, PacketType.REGISTER, {"username": username, "password": password})
        response = await receive_packet(self.reader)
        await self.disconnect()

        if response:
            return response[1]["success"], response[1]["message"]
        return False, "No response from server"

    async def login(self, username, password):
        if not await self.connect():
            return False, "Could not connect to server"

        await send_packet(self.writer, PacketType.LOGIN, {"username": username, "password": password})
        response = await receive_packet(self.reader)

        if response and response[0] == PacketType.LOGIN_RESPONSE:
            if response[1]["success"]:
                self.username = username
                return True, response[1]["message"]
            else:
                await self.disconnect()
                return False, response[1]["message"]

        await self.disconnect()
        return False, "Invalid response from server"

    async def send_message(self, message):
        if self.writer:
            await send_packet(self.writer, PacketType.MESSAGE, {"message": message})

    async def listen_for_packets(self):
        try:
            while self.reader:
                packet = await receive_packet(self.reader)
                if packet is None:
                    self.logger.info("Server closed connection.")
                    break

                packet_type, payload = packet
                if packet_type == PacketType.MESSAGE:
                    msg = Message.from_dict(payload)
                    await self.ui.display_message(msg)
                elif packet_type == PacketType.HISTORY:
                    messages = [Message.from_dict(m) for m in payload["messages"]]
                    await self.ui.display_history(messages)
                elif packet_type == PacketType.SYSTEM:
                    await self.ui.display_system_message(payload["message"])

        except asyncio.CancelledError:
            pass
        except Exception as e:
            self.logger.error(f"Error listening for packets: {e}")
        finally:
            self.logger.info("Stopped listening for packets.")
            if self.ui.in_chat:
                await self.ui.display_system_message("Disconnected from server.")
                self.ui.in_chat = False

async def main():
    client = ChatClient()
    client.logger.info("Starting ResiliChat Client")

    ui = ChatUI(client)
    client.ui = ui
    ui.display_banner()

    try:
        await ui.main_menu()
    except KeyboardInterrupt:
        pass
    finally:
        await client.disconnect()
        client.logger.info("Client shut down")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(0)
