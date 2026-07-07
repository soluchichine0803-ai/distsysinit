import aioconsole
import sys

BANNER = """
****************************************************************
*                                                              *
*                      RESILICHAT CLIENT                       *
*               Distributed Systems Chat Project               *
*                                                              *
****************************************************************
"""

class ChatUI:
    def __init__(self):
        self.running = True

    def display_banner(self):
        print(BANNER)

    async def main_menu(self):
        while self.running:
            print("\n--- MAIN MENU ---")
            print("1. Login")
            print("2. Register")
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
                print("Invalid choice, please try again.")

    async def login_screen(self):
        print("\n--- LOGIN ---")
        username = await aioconsole.ainput("Username: ")
        password = await aioconsole.ainput("Password: ", use_captured_char=True)
        print(f"\n[PHASE 1] Login attempted for {username} (Networking not implemented yet)")

    async def register_screen(self):
        print("\n--- REGISTER ---")
        username = await aioconsole.ainput("Username: ")
        password = await aioconsole.ainput("Password: ", use_captured_char=True)
        print(f"\n[PHASE 1] Registration attempted for {username} (Networking not implemented yet)")

    async def settings_screen(self):
        print("\n--- SETTINGS ---")
        print("[PHASE 1] No settings available yet.")
