import socket
import json
import asyncio
from typing import Any, Dict, List, Optional, Union

class VeloxDBError(Exception):
    pass

class VeloxDB:
    """Synchronous Python client for VeloxDB."""

    def __init__(self, host: str = "127.0.0.1", port: int = 7379, password: Optional[str] = None, table: str = "players"):
        self.host = host
        self.port = port
        self.password = password
        self.table = table
        self._sock: Optional[socket.socket] = None
        self._connect()

    def _connect(self):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self._sock.connect((self.host, self.port))
        if self.password:
            self.execute(f"AUTH {self.password}")

    def _send_line(self, cmd: str) -> Any:
        if not cmd.endswith("\r\n"):
            cmd += "\r\n"
        self._sock.sendall(cmd.encode("utf-8"))
        f = self._sock.makefile("r", encoding="utf-8")
        line = f.readline()
        if not line:
            raise VeloxDBError("Connection closed by server")
        trimmed = line.strip()
        if trimmed.startswith("-ERR"):
            raise VeloxDBError(trimmed[5:])
        if trimmed.startswith("+"):
            return trimmed[1:]
        if trimmed.startswith(":"):
            return int(trimmed[1:])
        if trimmed.startswith("$"):
            length = int(trimmed[1:])
            if length == -1:
                return None
            data = f.read(length)
            f.read(2)  # consume CRLF
            return data
        return trimmed

    def execute(self, cmd: str) -> Any:
        return self._send_line(cmd)

    def set(self, key: str, value: Union[str, Dict, List], table: Optional[str] = None) -> bool:
        t = table or self.table
        val_str = json.dumps(value) if isinstance(value, (dict, list)) else str(value)
        res = self._send_line(f"SET {t} {key} {val_str}")
        return res == "OK"

    def get(self, key: str, table: Optional[str] = None) -> Optional[Any]:
        t = table or self.table
        raw = self._send_line(f"GET {t} {key}")
        if raw is None:
            return None
        try:
            return json.loads(raw)
        except Exception:
            return raw

    def json_set(self, key: str, path: str, value: Any, table: Optional[str] = None) -> bool:
        t = table or self.table
        val_str = json.dumps(value)
        res = self._send_line(f"JSON.SET {t} {key} {path} {val_str}")
        return res == "OK"

    def top(self, path: str, limit: int = 10, table: Optional[str] = None) -> List[Dict]:
        t = table or self.table
        raw = self._send_line(f"TOP {t} {path} {limit}")
        return json.loads(raw) if raw else []

    def count(self, query: Optional[str] = None, table: Optional[str] = None) -> int:
        t = table or self.table
        cmd = f"COUNT {t} {query}" if query else f"COUNT {t}"
        return int(self._send_line(cmd))

    def delete(self, key: str, table: Optional[str] = None) -> bool:
        t = table or self.table
        res = self._send_line(f"DEL {t} {key}")
        return res == 1 or res == "1"

    def close(self):
        if self._sock:
            self._sock.close()


class AsyncVeloxDB:
    """Asynchronous asyncio Python client for VeloxDB."""

    def __init__(self, host: str = "127.0.0.1", port: int = 7379, password: Optional[str] = None, table: str = "players"):
        self.host = host
        self.port = port
        self.password = password
        self.table = table
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None

    async def connect(self):
        self._reader, self._writer = await asyncio.open_connection(self.host, self.port)
        if self.password:
            await self._send_line(f"AUTH {self.password}")

    async def _send_line(self, cmd: str) -> Any:
        if not cmd.endswith("\r\n"):
            cmd += "\r\n"
        self._writer.write(cmd.encode("utf-8"))
        await self._writer.drain()

        line = await self._reader.readline()
        if not line:
            raise VeloxDBError("Connection closed by server")
        trimmed = line.decode("utf-8").strip()
        if trimmed.startswith("-ERR"):
            raise VeloxDBError(trimmed[5:])
        if trimmed.startswith("+"):
            return trimmed[1:]
        if trimmed.startswith(":"):
            return int(trimmed[1:])
        if trimmed.startswith("$"):
            length = int(trimmed[1:])
            if length == -1:
                return None
            data = await self._reader.readexactly(length)
            await self._reader.readexactly(2)  # consume CRLF
            return data.decode("utf-8")
        return trimmed

    async def set(self, key: str, value: Union[str, Dict, List], table: Optional[str] = None) -> bool:
        t = table or self.table
        val_str = json.dumps(value) if isinstance(value, (dict, list)) else str(value)
        res = await self._send_line(f"SET {t} {key} {val_str}")
        return res == "OK"

    async def get(self, key: str, table: Optional[str] = None) -> Optional[Any]:
        t = table or self.table
        raw = await self._send_line(f"GET {t} {key}")
        if raw is None:
            return None
        try:
            return json.loads(raw)
        except Exception:
            return raw

    async def top(self, path: str, limit: int = 10, table: Optional[str] = None) -> List[Dict]:
        t = table or self.table
        raw = await self._send_line(f"TOP {t} {path} {limit}")
        return json.loads(raw) if raw else []

    async def close(self):
        if self._writer:
            self._writer.close()
            await self._writer.wait_closed()


# Backwards compatibility aliases
MeowDB = VeloxDB
AsyncMeowDB = AsyncVeloxDB
