import random
import threading
from typing import Any, List
from labrpc.labrpc import ClientEnd
from server import GetArgs, GetReply, PutAppendArgs, PutAppendReply

def nrand() -> int:
    return random.getrandbits(62)

class Clerk:
    def __init__(self, servers: List[ClientEnd], cfg):
        self.servers = servers
        self.cfg = cfg
        self.client_id = nrand()
        self.seq_num = 0
        self.nservers = len(servers)
        self.nreplicas = getattr(cfg, "nreplicas", 1)
        self.nshards = getattr(cfg, "nservers", 1)
        self.lock = threading.Lock()

    def _next_seq(self):
        with self.lock:
            seq = self.seq_num
            self.seq_num += 1
            return seq

    def _shard_for_key(self, key):
        try:
            return int(key) % self.nshards
        except Exception:
            return hash(key) % self.nshards

    def get(self, key: str) -> str:
        seq = self._next_seq()
        args = GetArgs(key)
        args.client_id = self.client_id
        args.seq_num = seq
        shard = self._shard_for_key(key)
        for attempt in range(10000):
            for rep in range(self.nreplicas):
                server_idx = (shard + rep) % self.nservers
                try:
                    reply = self.servers[server_idx].call("KVServer.Get", args)
                    if reply is not None and hasattr(reply, "value"):
                        return reply.value if reply.value is not None else ""
                except TimeoutError:
                    continue
        return ""

    def put_append(self, key: str, value: str, op: str) -> str:
        seq = self._next_seq()
        args = PutAppendArgs(key, value)
        args.client_id = self.client_id
        args.seq_num = seq
        args.op = op
        shard = self._shard_for_key(key)
        result = None
        # Try all replicas for the shard to ensure all are up-to-date
        for rep in range(self.nreplicas):
            server_idx = (shard + rep) % self.nservers
            try:
                reply = self.servers[server_idx].call(f"KVServer.{op}", args)
                if reply is not None and hasattr(reply, "value"):
                    result = reply.value if reply.value is not None else ""
            except TimeoutError:
                continue
        return result if result is not None else ""

    def put(self, key: str, value: str):
        self.put_append(key, value, "Put")

    def append(self, key: str, value: str) -> str:
        return self.put_append(key, value, "Append")
