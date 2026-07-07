import asyncio
import logging
from database.postgres import PostgresManager
from database.persistence import PersistenceManager
from shared.utils import setup_logger

class Server:
    def __init__(self, server_id: int, is_leader: bool = False):
        self.server_id = server_id
        self.is_leader = is_leader

        # Initialize logger
        role = "Leader" if is_leader else f"Follower-{server_id}"
        self.logger = setup_logger(role)

        # Initialize database
        self.db = PostgresManager()
        self.persistence = PersistenceManager(self.db)

    async def start(self):
        self.logger.info(f"Starting {self.__class__.__name__} with ID {self.server_id} (Leader: {self.is_leader})")

        # Connect to DB and initialize tables
        try:
            await self.db.connect()
            await self.persistence.initialize_tables()
            self.logger.info("Database initialized successfully")
        except Exception as e:
            self.logger.error(f"Failed to initialize database: {e}")
            # In Phase 1, we continue even if DB fails to allow testing without DB
            self.logger.warning("Continuing without database connection for Phase 1 verification")

    async def stop(self):
        self.logger.info(f"Shutting down server {self.server_id}")
        await self.db.disconnect()
