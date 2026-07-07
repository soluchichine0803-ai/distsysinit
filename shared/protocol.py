import json
import struct
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
