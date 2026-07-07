import asyncpg
import logging
from shared.config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
from shared.utils import setup_logger

class PostgresManager:
    def __init__(self):
        self.logger = setup_logger("Database")
        self.pool = None

    async def connect(self):
        """Initializes the connection pool."""
        try:
            self.pool = await asyncpg.create_pool(
                host=DB_HOST,
                port=DB_PORT,
                user=DB_USER,
                password=DB_PASSWORD,
                database=DB_NAME
            )
            self.logger.info(f"Connected to PostgreSQL at {DB_HOST}:{DB_PORT}")
        except Exception as e:
            self.logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise

    async def disconnect(self):
        """Closes the connection pool."""
        if self.pool:
            await self.pool.close()
            self.logger.info("Disconnected from PostgreSQL")

    async def execute(self, query: str, *args):
        """Executes a query."""
        async with self.pool.acquire() as conn:
            return await conn.execute(query, *args)

    async def fetch(self, query: str, *args):
        """Fetches rows."""
        async with self.pool.acquire() as conn:
            return await conn.fetch(query, *args)

    async def fetchrow(self, query: str, *args):
        """Fetches a single row."""
        async with self.pool.acquire() as conn:
            return await conn.fetchrow(query, *args)
