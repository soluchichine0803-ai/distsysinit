import aioconsole
import sys
import asyncio
from datetime import datetime

BANNER = """
=========================
      RESILICHAT
=========================
"""

class ChatUI:
    def __init__(self, client):
        self.client = client
        self.running = True
        self.in_chat = False

    def clear_screen(self):
        print("\033[2J\033[H", end="")

    def display_banner(self):
        print(BANNER)
        print(f"Server IP: {self.client.host}")
        print(f"Server Port: {self.client.port}")

    async def main_menu(self):
        while self.running:
            self.clear_screen()
            self.display_banner()
            print("\n1. Login")
            print("2. Create Account")
            print("3. Settings")
            print("4. Exit")

            choice = await aioconsole.ainput("\nSelect an option: ")

            if choice == "1":
                await self.login_screen()
            elif choice == "2":
                await self.register_screen()
            elif choice == "3":
                await self.settings_screen()
            elif choice == "4":
                print("Exiting...")
                self.running = False
            else:
                print("Invalid choice, press Enter to try again.")
                await aioconsole.ainput()

    async def login_screen(self):
        self.clear_screen()
        print("=========================")
        print("         LOGIN")
        print("=========================")
        username = await aioconsole.ainput("Username: ")
        password = await aioconsole.ainput("Password: ") # aioconsole doesn't support mask well easily without extra libs, requirements say raw ANSI etc.

        success, message = await self.client.login(username, password)
        if success:
            print(f"\n*** SYSTEM: {message} ***")
            await asyncio.sleep(1)
            await self.chat_screen()
        else:
            print(f"\n*** SYSTEM: Login failed - {message} ***")
            await aioconsole.ainput("\nPress Enter to return to menu...")

    async def register_screen(self):
        self.clear_screen()
        print("=========================")
        print("      CREATE ACCOUNT")
        print("=========================")
        username = await aioconsole.ainput("Username: ")
        password = await aioconsole.ainput("Password: ")

        success, message = await self.client.register(username, password)
        if success:
            print(f"\n*** SYSTEM: {message} ***")
        else:
            print(f"\n*** SYSTEM: Registration failed - {message} ***")
        await aioconsole.ainput("\nPress Enter to return to menu...")

    async def settings_screen(self):
        self.clear_screen()
        print("=========================")
        print("        SETTINGS")
        print("=========================")
        print(f"Current Server: {self.client.host}:{self.client.port}")

        new_host = await aioconsole.ainput("New Server IP (leave blank to keep): ")
        if new_host:
            self.client.host = new_host

        new_port = await aioconsole.ainput("New Server Port (leave blank to keep): ")
        if new_port:
            try:
                self.client.port = int(new_port)
            except ValueError:
                print("Invalid port, keeping current.")

        if new_host or new_port:
            self.client.save_config()
            print("\nSettings saved.")

        await aioconsole.ainput("\nPress Enter to return to menu...")

    async def chat_screen(self):
        self.clear_screen()
        print("================================")
        print("Current Session")
        print(datetime.now().strftime("%Y-%m-%d %H:%M"))
        print("================================\n")

        self.in_chat = True

        # Start background listener for incoming messages
        listener_task = asyncio.create_task(self.client.listen_for_packets())

        print("Previous Chat History")
        print("--------------------------------")
        # History will be printed by display_history called from listener

        # Wait a bit for history to arrive
        await asyncio.sleep(0.5)

        print("--------------------------------")
        print("\nLive Chat\n")

        try:
            while self.in_chat:
                message = await aioconsole.ainput("> ")
                if not self.in_chat:
                    break
                if message.strip().lower() == "/exit" or message.strip().lower() == "/logout":
                    self.in_chat = False
                    break
                if message.strip():
                    await self.client.send_message(message)
        except Exception as e:
            print(f"\nError in chat: {e}")
        finally:
            self.in_chat = False
            listener_task.cancel()
            await self.client.disconnect()
            print("\nLogged out.")
            await asyncio.sleep(1)

    async def display_message(self, msg):
        ts = msg.timestamp.strftime("%H:%M")
        print(f"\n[{ts}]\n{msg.username}:\n{msg.message}\n")
        print("> ", end="", flush=True)

    async def display_history(self, messages):
        for msg in messages:
            ts = msg.timestamp.strftime("%H:%M")
            print(f"[{ts}] {msg.username}: {msg.message}")
        # After history is displayed, if we are in chat, we might need to reprint the prompt
        # but display_history is called during chat_screen setup mostly.

    async def display_system_message(self, message):
        print(f"\n*** SYSTEM: {message} ***")
        if self.in_chat:
            print("> ", end="", flush=True)
