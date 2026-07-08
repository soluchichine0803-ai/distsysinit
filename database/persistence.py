from database.postgres import PostgresManager
from shared.models import User, ServerInfo, AuditEvent
from datetime import datetime

class PersistenceManager:
    def __init__(self, db: PostgresManager):
        self.db = db

    async def initialize_tables(self):
        """Creates tables if they don't exist."""

        # Users table
        await self.db.execute("""
            CREATE TABLE IF NOT EXISTS Users (
                user_id SERIAL PRIMARY KEY,
                username TEXT UNIQUE NOT NULL,
                password TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        # Servers table
        await self.db.execute("""
            CREATE TABLE IF NOT EXISTS Servers (
                server_id INTEGER PRIMARY KEY,
                host TEXT,
                port INTEGER,
                status TEXT,
                is_leader BOOLEAN,
                election_order INTEGER,
                last_heartbeat TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        # AuditLogs table
        await self.db.execute("""
            CREATE TABLE IF NOT EXISTS AuditLogs (
                log_id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                server_id INTEGER,
                event TEXT,
                details TEXT
            );
        """)

    # Helper methods for later use
    async def create_user(self, user: User):
        query = "INSERT INTO Users (username, password, created_at) VALUES ($1, $2, $3) RETURNING user_id"
        row = await self.db.fetchrow(query, user.username, user.password, user.created_at)
        return row['user_id'] if row else None

    async def get_user_by_username(self, username: str):
        query = "SELECT * FROM Users WHERE username = $1"
        return await self.db.fetchrow(query, username)

    async def update_server_status(self, server: ServerInfo):
        query = """
            INSERT INTO Servers (server_id, host, port, status, is_leader, election_order, last_heartbeat, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (server_id) DO UPDATE SET
                host = EXCLUDED.host,
                port = EXCLUDED.port,
                status = EXCLUDED.status,
                is_leader = EXCLUDED.is_leader,
                election_order = EXCLUDED.election_order,
                last_heartbeat = EXCLUDED.last_heartbeat,
                updated_at = EXCLUDED.updated_at
        """
        await self.db.execute(
            query,
            server.server_id, server.host, server.port, server.status,
            server.is_leader, server.election_order, server.last_heartbeat, server.updated_at
        )

    async def add_audit_log(self, event: AuditEvent):
        query = "INSERT INTO AuditLogs (timestamp, server_id, event, details) VALUES ($1, $2, $3, $4)"
        await self.db.execute(query, event.timestamp, event.server_id, event.event, event.details)

    async def get_leader(self):
        query = "SELECT * FROM Servers WHERE is_leader = TRUE AND status = 'ONLINE' LIMIT 1"
        return await self.db.fetchrow(query)

    async def get_active_servers(self):
        query = "SELECT * FROM Servers WHERE status = 'ONLINE' ORDER BY election_order ASC"
        return await self.db.fetch(query)

    async def get_max_election_order(self):
        query = "SELECT MAX(election_order) FROM Servers"
        row = await self.db.fetchrow(query)
        return row['max'] if row and row['max'] is not None else 0

    async def get_server_by_id(self, server_id: int):
        query = "SELECT * FROM Servers WHERE server_id = $1"
        return await self.db.fetchrow(query, server_id)
