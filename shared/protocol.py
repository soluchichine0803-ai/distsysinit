import json
import struct
import asyncio
from enum import Enum, auto

class PacketType(Enum):
    LOGIN = "LOGIN"
    REGISTER = "REGISTER"
    LOGIN_RESPONSE = "LOGIN_RESPONSE"
    REGISTER_RESPONSE = "REGISTER_RESPONSE"
    MESSAGE = "MESSAGE"
    HISTORY = "HISTORY"
    HEARTBEAT = "HEARTBEAT"
    HEARTBEAT_ACK = "HEARTBEAT_ACK"
    REPLICATION = "REPLICATION"
    FAILOVER = "FAILOVER"
    SYSTEM = "SYSTEM"
    SERVER_JOIN = "SERVER_JOIN"

def create_packet(packet_type: PacketType, payload: dict) -> bytes:
    """
    Creates a packet with a 4-byte big-endian length header followed by JSON data.
    """
    data = {
        "type": packet_type.value,
        "payload": payload
    }
    json_str = json.dumps(data)
    json_bytes = json_str.encode('utf-8')
    length = len(json_bytes)
    header = struct.pack('>I', length)
    return header + json_bytes

def parse_packet(data: bytes) -> tuple[int, dict | None]:
    """
    Parses a packet. Returns (bytes_consumed, packet_dict).
    If not enough data, returns (0, None).
    """
    if len(data) < 4:
        return 0, None

    length = struct.unpack('>I', data[:4])[0]
    if len(data) < 4 + length:
        return 0, None

    json_bytes = data[4:4+length]
    json_str = json_bytes.decode('utf-8')
    packet_dict = json.loads(json_str)
    return 4 + length, packet_dict

async def send_packet(writer: asyncio.StreamWriter, packet_type: PacketType, payload: dict):
    """
    Sends a packet through the writer.
    """
    packet = create_packet(packet_type, payload)
    writer.write(packet)
    await writer.drain()

async def read_exactly(reader: asyncio.StreamReader, n: int) -> bytes:
    """
    Reads exactly n bytes from the reader.
    """
    data = b''
    while len(data) < n:
        chunk = await reader.read(n - len(data))
        if not chunk:
            raise ConnectionError("Connection closed while reading")
        data += chunk
    return data

async def receive_packet(reader: asyncio.StreamReader) -> tuple[PacketType, dict] | None:
    """
    Receives a packet from the reader.
    """
    try:
        header = await read_exactly(reader, 4)
        length = struct.unpack('>I', header)[0]
        payload_data = await read_exactly(reader, length)
        packet_dict = json.loads(payload_data.decode('utf-8'))
        return PacketType(packet_dict["type"]), packet_dict["payload"]
    except (ConnectionError, asyncio.IncompleteReadError):
        return None
    except Exception:
        return None
