# ResiliChat - Distributed Systems Chat Project

ResiliChat is a fully structured, fault-tolerant distributed chat system built with Python's `asyncio` and PostgreSQL.

## Project Overview

This project implements a Leader-Follower architecture for a chat system, ensuring high availability and data persistence.

## Architecture Overview

- **Leader**: Manages client connections, coordinates replication, and handles system state.
- **Follower**: Replicates data from the Leader and stands ready to take over in case of failure.
- **Client**: Provides a CLI interface for users to register, login, and chat.
- **Database**: PostgreSQL is used for persistent storage of users, server metadata, and audit logs.

## Directory Structure

```text
Distributed-Systems/

client/
    __init__.py
    client.py       # Client entry point
    ui.py           # CLI Interface

server/
    __init__.py
    leader.py       # Leader entry point
    follower.py     # Follower entry point
    server.py       # Core Server class
    heartbeat.py    # Heartbeat logic (Phase 2+)
    election.py     # Leader election (Phase 2+)
    replication.py  # Data replication (Phase 2+)

database/
    __init__.py
    postgres.py     # Connection management
    persistence.py  # Schema and CRUD helpers

shared/
    __init__.py
    config.py       # Configuration
    protocol.py     # Packet protocol
    models.py       # Dataclasses
    utils.py        # Logging and helpers

history/            # Replicated history files
logs/               # Component logs
requirements.txt    # Project dependencies
README.md           # Documentation
```

## Prerequisites

- Python 3.10+
- PostgreSQL

## Installation

1. Clone the repository.
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## PostgreSQL Setup

Ensure PostgreSQL is running and accessible with the following defaults (configurable in `shared/config.py`):

- **Host**: localhost
- **Port**: 5432
- **Database**: resilichat
- **User**: postgres
- **Password**: Soluchi

## Running the Project

### Starting the Leader

```bash
python3 -m server.leader --server-id 1
```

### Starting a Follower

```bash
python3 -m server.follower --server-id 2
```

### Starting the Client

```bash
python3 -m client.client
```

## Phase 1 Progress

- [x] Project structure established.
- [x] Dependencies defined.
- [x] Configuration and logging implemented.
- [x] Packet protocol (length-prefixed JSON) defined.
- [x] Database schema and connection management ready.
- [x] Server and Client entry points functional.
