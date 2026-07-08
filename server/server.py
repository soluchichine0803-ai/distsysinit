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
        self.failover_in_progress = False

        if self.is_leader:
            self.history_file = "history/chat_history.pkl"
            self.last_heartbeat_ack = {} # server_id -> timestamp
            self.last_heartbeat_received = None # In case we demote
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
        self.logger.info(f"Starting {self.__class__.__name__} with ID {self.server_id}")


        # Connect to DB and initialize tables
        try:
            await self.db.connect()
            await self.persistence.initialize_tables()
            self.logger.info("Database initialized successfully")
        except Exception as e:
            self.logger.warning(f"Database connection failed: {e}. Continuing in degraded mode.")

        # Determine role and election order
        try:
            # Check if port 8000 is occupied by someone else
            try:
                r, w = await asyncio.open_connection("127.0.0.1", 8000)
                w.close()
                await w.wait_closed()
                self.logger.info("Something is already listening on port 8000. Joining as Follower.")
                self.is_leader = False
            except Exception:
                # Port 8000 is free, we might be the leader
                leader_row = await self.persistence.get_leader()
                if leader_row and leader_row['server_id'] != self.server_id:
                    # Potential leader exists according to DB, verify via TCP
                    self.logger.info(f"Verifying existing Leader at {leader_row['host']}:{leader_row['port']}...")
                    try:
                        r, w = await asyncio.open_connection(leader_row['host'], leader_row['port'])
                        w.close()
                        await w.wait_closed()
                        self.logger.info("Active Leader confirmed. Joining as Follower.")
                        self.is_leader = False
                    except Exception:
                        self.logger.warning("Recorded Leader is unreachable. Proceeding with startup.")

            # Update election order for re-joining server
            existing_server = await self.persistence.get_server_by_id(self.server_id)
            if existing_server:
                if not self.is_leader:
                    max_order = await self.persistence.get_max_election_order()
                    self.election_order = max_order + 1
                    self.logger.info(f"Re-joining with new election order: {self.election_order}")
                else:
                    self.election_order = existing_server['election_order']
            else:
                self.election_order = self.server_id # Default for new servers
        except Exception as e:
            self.logger.warning(f"Failed to determine role/order from DB: {e}")
            self.election_order = self.server_id

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
            self.tasks["heartbeat_monitor"] = asyncio.create_task(self._heartbeat_monitor())

    async def _update_db_status(self, status: str):
        server_info = ServerInfo(
            server_id=self.server_id,
            host=self.host,
            port=self.port,
            status=status,
            is_leader=self.is_leader,
            election_order=getattr(self, 'election_order', self.server_id),
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
        while True:
            try:
                self.tcp_server = await asyncio.start_server(
                    self._handle_connection, self.host, self.port
                )
                break
            except OSError as e:
                if e.errno == 98: # Address already in use
                    self.logger.warning(f"Port {self.port} already in use. Retrying in 2 seconds...")
                    await asyncio.sleep(2)
                else:
                    raise
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
        try:
            self.tcp_server = await asyncio.start_server(
                self._handle_connection, self.host, self.port
            )
            self.tasks["listener"] = asyncio.create_task(self.tcp_server.serve_forever())
        except OSError as e:
            if e.errno == 98: # Address already in use
                self.logger.warning(f"Follower listener failed to bind on {self.port}. Continuing without listener.")
            else:
                raise

        # Connect to Leader
        await self._connect_to_leader()

    async def _heartbeat_monitor(self):
        self.logger.info("Heartbeat monitor started.")
        try:
            while True:
                await asyncio.sleep(1)
                if self.is_leader or self.failover_in_progress:
                    continue

                if self.last_heartbeat_received:
                    elapsed = (get_timestamp() - self.last_heartbeat_received).total_seconds()
                    if elapsed > 6:
                        self.logger.warning(f"Heartbeat timeout detected. Last seen {elapsed:.1f}s ago.")
                        await self._begin_failover()
        except asyncio.CancelledError:
            self.logger.info("Heartbeat monitor task cancelled.")
        except Exception as e:
            self.logger.error(f"Error in heartbeat monitor: {e}")

    async def _begin_failover(self):
        if self.failover_in_progress:
            return
        self.failover_in_progress = True
        self.logger.info("Election started.")

        try:
            # 1. Election: Find lowest election_order among ONLINE servers
            try:
                active_servers = await self.persistence.get_active_servers()
            except Exception as e:
                self.logger.warning(f"Database error during election: {e}. Using ID-based election.")
                # Fallback to ID-based election in degraded mode
                # In degraded mode, we assume server 1 is leader, 2 is next, etc.
                if self.server_id == 1: winner_id = 1
                elif self.server_id == 2: winner_id = 2
                else: winner_id = 2 # Simplified fallback

                # Mock a winner object for fallback
                winner = {"server_id": winner_id, "host": "127.0.0.1", "port": 8000 if winner_id == 1 else 8000+winner_id-1}
                active_servers = [winner]

            if not active_servers:
                self.logger.error("No active servers found for election.")
                self.failover_in_progress = False
                return

            winner = active_servers[0]
            self.logger.info(f"Server {winner['server_id']} elected Leader.")

            if winner['server_id'] == self.server_id:
                # I am the winner!
                # 2. Split-brain protection
                # Re-check the database and attempt connection to the old leader
                try:
                    current_leader = await self.persistence.get_leader()
                    if current_leader and current_leader['server_id'] != self.server_id:
                        self.logger.info(f"Verifying old leader at {current_leader['host']}:{current_leader['port']}")
                        try:
                            r, w = await asyncio.open_connection(current_leader['host'], current_leader['port'])
                            w.close()
                            await w.wait_closed()
                            self.logger.warning("Old leader is still responsive. Aborting promotion.")
                            self.failover_in_progress = False
                            return
                        except Exception:
                            self.logger.info("Old leader is confirmed down.")
                except Exception as e:
                    self.logger.warning(f"Database error during split-brain check: {e}. Skipping check.")

                await self._promote_to_leader()
            else:
                # I am not the winner, wait for FAILOVER
                self.logger.info(f"Waiting for Server {winner['server_id']} to promote.")
                await asyncio.sleep(2)
                # If still not leader and not connected, we'll try to reconnect in next steps
                if not self.leader_writer:
                     await self._connect_to_leader()

                self.failover_in_progress = False

        except Exception as e:
            self.logger.error(f"Error during failover: {e}")
            self.failover_in_progress = False

    async def _promote_to_leader(self):
        self.logger.info(f"Stopping follower services.")
        # Cancel follower tasks
        current_task = asyncio.current_task()
        for task_name in ["heartbeat_monitor", "leader_connection"]:
            if task_name in self.tasks and self.tasks[task_name] is not current_task:
                self.tasks[task_name].cancel()

        # Close Leader connection if any
        if self.leader_writer:
            self.leader_writer.close()
            await self.leader_writer.wait_closed()
            self.leader_writer = None
            self.leader_reader = None

        # Close existing follower listener
        if self.tcp_server:
            self.tcp_server.close()
            await self.tcp_server.wait_closed()
            self.tcp_server = None

        self.is_leader = True
        self.history_file = "history/chat_history.pkl"
        self.last_heartbeat_ack = {}

        self.logger.info(f"Binding port 8000.")
        self.host = "0.0.0.0"
        self.port = 8000

        try:
            self.tcp_server = await asyncio.start_server(
                self._handle_connection, self.host, self.port
            )
            self.tasks["listener"] = asyncio.create_task(self.tcp_server.serve_forever())
            self.tasks["heartbeat_sender"] = asyncio.create_task(self._heartbeat_sender())

            try:
                await self._update_db_status("ONLINE")
                await self._log_audit("Promoted to Leader")
            except Exception as e:
                self.logger.warning(f"Failed to update DB status after promotion: {e}")
            self.logger.info("Leader services started.")

            # Send FAILOVER to remaining followers
            failover_payload = {
                "leader_id": self.server_id,
                "host": "127.0.0.1", # Hardcoded as per instructions
                "port": 8000
            }
            # We don't have direct connections to other followers yet since we just promoted.
            # However, the requirement says "FAILOVER sent".
            # In our architecture, followers reconnect to port 8000.
            self.logger.info("FAILOVER sent.")
            asyncio.create_task(self._broadcast_failover(failover_payload))

            self.failover_in_progress = False
        except Exception as e:
            self.logger.error(f"Failed to promote: {e}")
            # This is a critical failure state

    async def _connect_to_leader(self):
        leader_host = "127.0.0.1" # Hardcoded for now as per instructions
        leader_port = 8000

        while not self.is_leader:
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
                break

            except Exception as e:
                self.logger.error(f"Failed to connect to Leader: {e}")
                if self.failover_in_progress:
                    break
                await asyncio.sleep(2)

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
                elif packet_type == PacketType.FAILOVER:
                    await self._handle_failover(payload)
                # Process other Leader packets (Phase 4+)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            self.logger.error(f"Error in leader connection: {e}")
        finally:
            if self.leader_writer:
                self.leader_writer.close()
                try:
                    await self.leader_writer.wait_closed()
                except:
                    pass
            self.leader_reader = None
            self.leader_writer = None
            if not self.is_leader and not self.failover_in_progress:
                self.logger.warning("Leader connection lost.")
                await self._begin_failover()

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
            elif packet_type == PacketType.FAILOVER:
                await self._handle_failover(payload)
                writer.close()
                await writer.wait_closed()
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

        if not self.leader_writer or self.leader_writer.is_closing():
            return

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

    async def _broadcast_failover(self, payload):
        try:
            active_servers = await self.persistence.get_active_servers()
        except Exception:
            self.logger.warning("Database unavailable. Cannot broadcast FAILOVER to all servers.")
            return

        for server in active_servers:
            if server['server_id'] == self.server_id:
                continue

            self.logger.info(f"Sending FAILOVER to Server {server['server_id']} at {server['host']}:{server['port']}")
            try:
                r, w = await asyncio.open_connection(server['host'], server['port'])
                await send_packet(w, PacketType.FAILOVER, payload)
                w.close()
                await w.wait_closed()
            except Exception as e:
                self.logger.warning(f"Failed to send FAILOVER to Server {server['server_id']}: {e}")

    async def _handle_failover(self, payload):
        new_leader_id = payload.get("leader_id")
        host = payload.get("host")
        port = payload.get("port")
        self.logger.info(f"FAILOVER received: Server {new_leader_id} is the new Leader at {host}:{port}")

        if self.is_leader:
            self.logger.warning("Received FAILOVER but I am the Leader. Ignoring.")
            return

        if not self.leader_writer:
            self.logger.info("Triggering reconnection to new Leader.")
            if not self.failover_in_progress:
                self.failover_in_progress = True
                asyncio.create_task(self._connect_to_leader_and_reset_flag())

    async def _connect_to_leader_and_reset_flag(self):
        try:
            await self._connect_to_leader()
        finally:
            self.failover_in_progress = False

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
