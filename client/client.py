import asyncio
import sys
from client.ui import ChatUI
from shared.utils import setup_logger

async def main():
    logger = setup_logger("Client")
    logger.info("Starting ResiliChat Client")

    ui = ChatUI()
    ui.display_banner()

    try:
        await ui.main_menu()
    except KeyboardInterrupt:
        print("\nExiting...")
    finally:
        logger.info("Client shut down")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(0)
