import asyncio
import logging
import pickle
import os
from datetime import datetime
from database.postgres import PostgresManager
from database.persistence import PersistenceManager
from shared.utils import setup_logger, get_timestamp
from shared.models import ServerInfo, AuditEvent, User, Message
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
        self.connected_clients = {}    # username -> {reader, writer}
        self.leader_reader = None
        self.leader_writer = None
        self.tcp_server = None
        self.chat_history = []

        if self.is_leader:
            self.history_file = "history/chat_history.pkl"
            self.last_heartbeat_ack = {} # server_id -> timestamp
        else:
            self.history_file = f"history/chat_history_{self.server_id}.pkl"
            self.last_heartbeat_received = None

    def _load_chat_history(self):
        if os.path.exists(self.history_file):
            try:
                with open(self.history_file, 'rb') as f:
                    self.chat_history = pickle.load(f)
                self.logger.info(f"Loaded {len(self.chat_history)} messages from history.")
            except Exception as e:
                self.logger.error(f"Failed to load chat history: {e}")

    def _save_chat_history(self):
        try:
            os.makedirs(os.path.dirname(self.history_file), exist_ok=True)
            with open(self.history_file, 'wb') as f:
                pickle.dump(self.chat_history, f)
        except Exception as e:
            self.logger.error(f"Failed to save chat history: {e}")

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

        self._load_chat_history()
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
        if not self.db or not self.db.pool:
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
        self.tasks["heartbeat_sender"] = asyncio.create_task(self._heartbeat_sender())

    async def _heartbeat_sender(self):
        self.logger.info("Heartbeat loop started.")
        try:
            while True:
                await asyncio.sleep(2)
                if not self.connected_followers:
                    continue

                heartbeat_payload = {
                    "leader_id": self.server_id,
                    "timestamp": get_timestamp().isoformat()
                }

                # Copy items to list to avoid dictionary change during iteration
                for fid, info in list(self.connected_followers.items()):
                    try:
                        await send_packet(info["writer"], PacketType.HEARTBEAT, heartbeat_payload)
                        # self.logger.debug(f"Heartbeat sent to Follower {fid}")
                    except Exception as e:
                        self.logger.error(f"Failed to send heartbeat to Follower {fid}: {e}")
        except asyncio.CancelledError:
            self.logger.info("Heartbeat sender task cancelled.")

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

                packet_type, payload = packet
                if packet_type == PacketType.HEARTBEAT:
                    await self._handle_heartbeat(payload)
                elif packet_type == PacketType.HISTORY:
                    await self._handle_history_sync(payload)
                elif packet_type == PacketType.REPLICATION:
                    await self._handle_replication(payload)
                # Process other Leader packets (Phase 4+)
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

        # Track connection task
        task = asyncio.current_task()
        task_id = f"conn_{addr[0]}_{addr[1]}"
        self.tasks[task_id] = task

        try:
            packet = await receive_packet(reader)
            if packet is None:
                return

            packet_type, payload = packet
            if packet_type == PacketType.SERVER_JOIN:
                await self._handle_follower_connection(reader, writer, payload)
            elif packet_type == PacketType.LOGIN:
                await self._handle_login(reader, writer, payload)
            elif packet_type == PacketType.REGISTER:
                await self._handle_register(reader, writer, payload)
            else:
                self.logger.warning(f"Received unexpected initial packet type {packet_type} from {addr}")
                writer.close()
                await writer.wait_closed()
        except Exception as e:
            self.logger.error(f"Error identifying connection from {addr}: {e}")
            writer.close()
            await writer.wait_closed()
        finally:
            self.tasks.pop(task_id, None)

    async def _handle_follower_connection(self, reader, writer, payload):
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

        # Track follower handler task
        task = asyncio.current_task()
        self.tasks[f"follower_{follower_id}"] = task

        # Send history snapshot immediately after join
        await self._send_history(writer)
        self.logger.info(f"History synchronized to Follower {follower_id}.")
        await self._log_audit("History synchronized", f"Sent to Follower {follower_id}")

        try:
            while True:
                packet = await receive_packet(reader)
                if packet is None:
                    break

                packet_type, payload = packet
                if packet_type == PacketType.HEARTBEAT_ACK:
                    await self._handle_heartbeat_ack(follower_id, payload)
                elif packet_type == PacketType.HISTORY:
                    await self._handle_history_sync(payload)
                # Process other Follower packets (Phase 4+)
        except Exception as e:
            self.logger.error(f"Error in follower {follower_id} connection: {e}")
        finally:
            if follower_id in self.connected_followers:
                del self.connected_followers[follower_id]
            self.tasks.pop(f"follower_{follower_id}", None)
            self.logger.info(f"Follower {follower_id} disconnected.")
            await self._log_audit("Follower Disconnected", f"Follower {follower_id} disconnected")
            writer.close()
            await writer.wait_closed()

    async def _handle_client_connection(self, reader, writer, username):
        self.connected_clients[username] = {
            "reader": reader,
            "writer": writer
        }
        self.logger.info(f"User {username} joined chat.")
        await self._log_audit("User joined chat", f"User: {username}")

        # Track client handler task
        task = asyncio.current_task()
        self.tasks[f"client_{username}"] = task

        # Restore history
        await self._send_history(writer)
        self.logger.info(f"History restored for {username}")

        try:
            while True:
                packet = await receive_packet(reader)
                if packet is None:
                    break

                packet_type, payload = packet
                if packet_type == PacketType.MESSAGE:
                    await self._handle_message(username, payload)
                else:
                    self.logger.warning(f"Unexpected packet {packet_type} from client {username}")
        except Exception as e:
            self.logger.error(f"Error in client {username} connection: {e}")
        finally:
            if username in self.connected_clients:
                del self.connected_clients[username]
            self.tasks.pop(f"client_{username}", None)
            self.logger.info(f"User {username} disconnected.")
            await self._log_audit("User disconnected", f"User: {username}")
            writer.close()
            await writer.wait_closed()

    async def _handle_login(self, reader, writer, payload):
        username = payload.get("username")
        password = payload.get("password")

        if not self.db.pool:
             # Degraded mode: allow login for testing if no DB, or just fail?
             # Let's just fail safely if DB is gone.
             await send_packet(writer, PacketType.LOGIN_RESPONSE, {"success": False, "message": "Database unavailable"})
             writer.close()
             await writer.wait_closed()
             return

        user_row = await self.persistence.get_user_by_username(username)
        if user_row and user_row['password'] == password:
            self.logger.info(f"User login success: {username}")
            await self._log_audit("User login success", f"User: {username}")
            await send_packet(writer, PacketType.LOGIN_RESPONSE, {"success": True, "message": "Login successful"})
            await self._handle_client_connection(reader, writer, username)
        else:
            self.logger.warning(f"User login failed: {username}")
            await self._log_audit("User login failed", f"User: {username}")
            await send_packet(writer, PacketType.LOGIN_RESPONSE, {"success": False, "message": "Invalid credentials"})
            writer.close()
            await writer.wait_closed()

    async def _handle_register(self, reader, writer, payload):
        username = payload.get("username")
        password = payload.get("password")

        existing_user = await self.persistence.get_user_by_username(username)
        if existing_user:
            self.logger.warning(f"Registration failed: Username {username} already exists.")
            await send_packet(writer, PacketType.REGISTER_RESPONSE, {"success": False, "message": "Username already exists"})
            writer.close()
            await writer.wait_closed()
            return

        try:
            user = User(username=username, password=password)
            await self.persistence.create_user(user)
            self.logger.info(f"User registered: {username}")
            await self._log_audit("User registered", f"User: {username}")
            await send_packet(writer, PacketType.REGISTER_RESPONSE, {"success": True, "message": "Registration successful"})
            # After registration, we don't automatically log in. Requirements say "Client sends: REGISTER -> Leader returns success/failure"
            # And then the client can login.
            writer.close()
            await writer.wait_closed()
        except Exception as e:
            self.logger.error(f"Error during registration for {username}: {e}")
            await send_packet(writer, PacketType.REGISTER_RESPONSE, {"success": False, "message": "Registration failed due to server error"})
            writer.close()
            await writer.wait_closed()

    async def _handle_message(self, username, payload):
        content = payload.get("message")
        msg = Message(username=username, message=content)

        # Store message
        self.chat_history.append(msg)
        self._save_chat_history()

        self.logger.info(f"Message broadcast from {username}")

        # Broadcast to clients
        await self._broadcast_message(msg)

        # Replicate to followers
        await self._replicate_message(msg)

    async def _broadcast_message(self, msg: Message):
        payload = msg.to_dict()
        disconnected_clients = []

        for username, info in self.connected_clients.items():
            try:
                await send_packet(info["writer"], PacketType.MESSAGE, payload)
            except Exception as e:
                self.logger.error(f"Failed to send message to {username}: {e}")
                disconnected_clients.append(username)

        for username in disconnected_clients:
            if username in self.connected_clients:
                del self.connected_clients[username]

    async def _replicate_message(self, msg: Message):
        payload = {"message": msg.to_dict()}
        disconnected_followers = []

        for fid, info in self.connected_followers.items():
            try:
                await send_packet(info["writer"], PacketType.REPLICATION, payload)
                self.logger.info(f"Replication sent to Server {fid}.")
                await self._log_audit("Replication sent", f"To Server {fid}")
            except Exception as e:
                self.logger.error(f"Failed to replicate message to Follower {fid}: {e}")
                disconnected_followers.append(fid)

        for fid in disconnected_followers:
            if fid in self.connected_followers:
                del self.connected_followers[fid]

    async def _send_history(self, writer):
        history_payload = {
            "messages": [msg.to_dict() for msg in self.chat_history]
        }
        await send_packet(writer, PacketType.HISTORY, history_payload)

    async def _handle_heartbeat(self, payload):
        # self.logger.debug("Heartbeat received.")
        self.last_heartbeat_received = datetime.fromisoformat(payload["timestamp"])

        ack_payload = {
            "server_id": self.server_id,
            "timestamp": get_timestamp().isoformat()
        }
        try:
            await send_packet(self.leader_writer, PacketType.HEARTBEAT_ACK, ack_payload)
            # self.logger.debug("Heartbeat ACK sent.")
        except Exception as e:
            self.logger.error(f"Failed to send heartbeat ACK: {e}")

    async def _handle_heartbeat_ack(self, follower_id, payload):
        # self.logger.debug(f"Heartbeat ACK received from Follower {follower_id}")
        self.last_heartbeat_ack[follower_id] = datetime.fromisoformat(payload["timestamp"])

    async def _handle_history_sync(self, payload):
        messages_data = payload.get("messages", [])
        self.chat_history = [Message.from_dict(m) for m in messages_data]
        self._save_chat_history()
        self.logger.info("History synchronized from Leader.")
        await self._log_audit("History synchronized", "Received from Leader")

    async def _handle_replication(self, payload):
        msg_data = payload.get("message")
        if msg_data:
            msg = Message.from_dict(msg_data)
            self.chat_history.append(msg)
            self._save_chat_history()
            self.logger.info("Replication received.")
            await self._log_audit("Replication received", f"From Leader")

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

        # Cancel and gather all tracked tasks
        if self.tasks:
            self.logger.debug(f"Cancelling {len(self.tasks)} tasks...")
            current_task = asyncio.current_task()
            tasks_to_cancel = [t for t in self.tasks.values() if t is not current_task]

            for task in tasks_to_cancel:
                task.cancel()

            await asyncio.gather(*tasks_to_cancel, return_exceptions=True)
            self.tasks.clear()

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
