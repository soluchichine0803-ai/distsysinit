import asyncio
import logging
from database.postgres import PostgresManager
from database.persistence import PersistenceManager
from shared.utils import setup_logger, get_timestamp
from shared.models import ServerInfo, AuditEvent
from shared.protocol import receive_packet, send_packet, PacketType

class Server:
    def __init__(self, server_id: int, host: str, port: int, is_leader: bool = False):
        self.server_id = server_id
        self.host = host
        self.port = port
        self.is_leader = is_leader

        # Initialize logger
        role = "Leader" if is_leader else f"Follower-{server_id}"
        self.logger = setup_logger(role)

        # Initialize database
        self.db = PostgresManager()
        self.persistence = PersistenceManager(self.db)

        # Networking state
        self.tasks = {}
        self.connected_followers = {}  # server_id -> {reader, writer, host, port}
        self.leader_reader = None
        self.leader_writer = None
        self.tcp_server = None

    async def start(self):
        self.logger.info(f"Starting {self.__class__.__name__} with ID {self.server_id} (Leader: {self.is_leader})")

        # Connect to DB and initialize tables
        try:
            await self.db.connect()
            await self.persistence.initialize_tables()
            self.logger.info("Database initialized successfully")
        except Exception as e:
            self.logger.warning(f"Database connection failed: {e}. Continuing in degraded mode.")

        # Register in DB
        try:
            await self._update_db_status("ONLINE")
        except Exception as e:
            self.logger.warning(f"Failed to register in DB: {e}")
        await self._log_audit(f"{'Leader' if self.is_leader else 'Follower'} Started")

        if self.is_leader:
            await self._start_leader_mode()
        else:
            await self._start_follower_mode()

    async def _update_db_status(self, status: str):
        server_info = ServerInfo(
            server_id=self.server_id,
            host=self.host,
            port=self.port,
            status=status,
            is_leader=self.is_leader,
            election_order=self.server_id, # Default election order to server_id for now
            last_heartbeat=get_timestamp()
        )
        await self.persistence.update_server_status(server_info)

    async def _log_audit(self, event: str, details: str = None):
        if not self.db.pool:
            return
        try:
            audit_event = AuditEvent(
                event=event,
                server_id=self.server_id,
                details=details,
                timestamp=get_timestamp()
            )
            await self.persistence.add_audit_log(audit_event)
        except Exception as e:
            self.logger.warning(f"Failed to log audit event: {e}")

    async def _start_leader_mode(self):
        self.logger.info(f"Leader listening on {self.host}:{self.port}")
        self.tcp_server = await asyncio.start_server(
            self._handle_connection, self.host, self.port
        )
        self.tasks["listener"] = asyncio.create_task(self.tcp_server.serve_forever())

    async def _start_follower_mode(self):
        self.logger.info(f"Follower {self.server_id} starting listener on {self.host}:{self.port}")
        # Followers also start a listener (for future client connections/failover)
        self.tcp_server = await asyncio.start_server(
            self._handle_connection, self.host, self.port
        )
        self.tasks["listener"] = asyncio.create_task(self.tcp_server.serve_forever())

        # Connect to Leader
        await self._connect_to_leader()

    async def _connect_to_leader(self):
        leader_host = "127.0.0.1" # Hardcoded for now as per instructions
        leader_port = 8000
        self.logger.info(f"Connecting to Leader at {leader_host}:{leader_port}...")
        try:
            self.leader_reader, self.leader_writer = await asyncio.open_connection(
                leader_host, leader_port
            )
            self.logger.info("Connected to Leader.")

            # Send SERVER_JOIN
            join_payload = {
                "server_id": self.server_id,
                "host": self.host,
                "port": self.port
            }
            await send_packet(self.leader_writer, PacketType.SERVER_JOIN, join_payload)
            self.logger.info("SERVER_JOIN sent.")
            await self._log_audit("Follower Connected", f"Connected to Leader at {leader_host}:{leader_port}")

            # Task to listen for messages from Leader
            self.tasks["leader_connection"] = asyncio.create_task(self._listen_to_leader())

        except Exception as e:
            self.logger.error(f"Failed to connect to Leader: {e}")

    async def _listen_to_leader(self):
        try:
            while True:
                packet = await receive_packet(self.leader_reader)
                if packet is None:
                    self.logger.info("Leader connection closed.")
                    break
                # Process Leader packets here in future phases
        except asyncio.CancelledError:
            pass
        except Exception as e:
            self.logger.error(f"Error in leader connection: {e}")
        finally:
            self.leader_reader = None
            self.leader_writer = None

    async def _handle_connection(self, reader, writer):
        addr = writer.get_extra_info('peername')
        self.logger.info(f"New connection from {addr}")

        try:
            while True:
                packet = await receive_packet(reader)
                if packet is None:
                    break

                packet_type, payload = packet
                if packet_type == PacketType.SERVER_JOIN:
                    await self._handle_server_join(reader, writer, payload)
                else:
                    self.logger.warning(f"Received unexpected packet type {packet_type} from {addr}")

        except Exception as e:
            self.logger.error(f"Error handling connection from {addr}: {e}")
        finally:
            # Check if this was a registered follower
            follower_id = None
            for fid, info in self.connected_followers.items():
                if info["writer"] == writer:
                    follower_id = fid
                    break

            if follower_id:
                del self.connected_followers[follower_id]
                self.logger.info(f"Follower {follower_id} disconnected.")
                await self._log_audit("Follower Disconnected", f"Follower {follower_id} disconnected")
            else:
                self.logger.info(f"Connection from {addr} closed.")

            writer.close()
            await writer.wait_closed()

    async def _handle_server_join(self, reader, writer, payload):
        follower_id = payload.get("server_id")
        host = payload.get("host")
        port = payload.get("port")

        self.logger.info(f"SERVER_JOIN received from Server {follower_id}.")
        self.connected_followers[follower_id] = {
            "reader": reader,
            "writer": writer,
            "host": host,
            "port": port
        }
        await self._log_audit("Follower Joined", f"Server {follower_id} joined from {host}:{port}")

    async def stop(self):
        self.logger.info(f"Shutting down server {self.server_id}")

        # Update DB status
        if self.db.pool:
            try:
                await self._update_db_status("OFFLINE")
                # Requirements say Leader = FALSE on shutdown for both roles
                query = "UPDATE Servers SET is_leader = FALSE WHERE server_id = $1"
                await self.db.execute(query, self.server_id)
                await self._log_audit("Server Shutdown")
            except Exception as e:
                self.logger.error(f"Failed to update DB status on shutdown: {e}")

        # Cancel tasks
        for name, task in self.tasks.items():
            self.logger.debug(f"Cancelling task {name}")
            task.cancel()

        if self.tasks:
            await asyncio.gather(*self.tasks.values(), return_exceptions=True)

        # Close TCP server
        if self.tcp_server:
            self.tcp_server.close()
            await self.tcp_server.wait_closed()

        # Close connections
        if self.leader_writer:
            self.leader_writer.close()
            await self.leader_writer.wait_closed()

        # Copy items to list to avoid dictionary change during iteration
        for fid, info in list(self.connected_followers.items()):
            info["writer"].close()
            await info["writer"].wait_closed()

        await self.db.disconnect()
        self.logger.info("Shutdown complete.")
