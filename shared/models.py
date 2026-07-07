from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

@dataclass
class Message:
    username: str
    message: str
    timestamp: datetime = field(default_factory=datetime.now)

    def to_dict(self):
        return {
            "username": self.username,
            "message": self.message,
            "timestamp": self.timestamp.isoformat()
        }

    @classmethod
    def from_dict(cls, data):
        return cls(
            username=data["username"],
            message=data["message"],
            timestamp=datetime.fromisoformat(data["timestamp"])
        )

@dataclass
class User:
    username: str
    password: str
    user_id: Optional[int] = None
    created_at: datetime = field(default_factory=datetime.now)

@dataclass
class ServerInfo:
    server_id: int
    host: str
    port: int
    status: str
    is_leader: bool
    election_order: int
    last_heartbeat: Optional[datetime] = None
    updated_at: datetime = field(default_factory=datetime.now)

@dataclass
class AuditEvent:
    event: str
    server_id: int
    details: Optional[str] = None
    timestamp: datetime = field(default_factory=datetime.now)
    log_id: Optional[int] = None
