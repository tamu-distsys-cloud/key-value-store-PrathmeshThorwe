import random
import threading
import time
from typing import Any, List
from labrpc.labrpc import ClientEnd
from server import GetArgs, GetReply, PutAppendArgs, PutAppendReply

def nrand() -> int:
    return random.getrandbits(62)

class Clerk:
    def __init__(self, servers: List[ClientEnd], cfg):
        self.servers = servers
        self.cfg = cfg
        self.client_id = nrand()  # Unique client identifier
        self.seq_num = 0
        self.nservers = len(servers)
        self.nreplicas = getattr(cfg, "nreplicas", 1)
        self.nshards = self.nservers  # In this lab, nshards = nservers
        self.lock = threading.Lock()

    def _next_seq(self):
        """Get next unique sequence number"""
        with self.lock:
            seq = self.seq_num
            self.seq_num += 1
            return seq

    def _shard_for_key(self, key):
        """Determine which shard a key belongs to"""
        try:
            return int(key) % self.nshards
        except ValueError:
            # Fallback for non-numeric keys
            return hash(key) % self.nshards

    def _servers_for_shard(self, shard):
        """Get list of server indices that handle the given shard"""
        servers = []
        for i in range(self.nreplicas):
            server_id = (shard + i) % self.nservers
            servers.append(server_id)
        return servers

    def get(self, key: str) -> str:
        """Fetch the current value for a key. Returns \"\" if the key does not exist."""
        seq = self._next_seq()
        args = GetArgs(key)
        args.client_id = self.client_id
        args.seq_num = seq

        shard = self._shard_for_key(key)
        servers = self._servers_for_shard(shard)

        # Keep trying until we get a successful response
        while True:
            for server_idx in servers:
                try:
                    reply = self.servers[server_idx].call("KVServer.Get", args)
                    if reply is not None and hasattr(reply, "value"):
                        return reply.value if reply.value is not None else ""
                except (TimeoutError, Exception):
                    continue
            
            # Brief pause before retrying all servers
            time.sleep(0.001)

    def put_append(self, key: str, value: str, op: str) -> str:
        """Shared implementation for Put and Append operations"""
        seq = self._next_seq()
        args = PutAppendArgs(key, value)
        args.client_id = self.client_id
        args.seq_num = seq
        args.op = op

        shard = self._shard_for_key(key)
        servers = self._servers_for_shard(shard)

        # Keep trying until we get a successful response
        while True:
            for server_idx in servers:
                try:
                    if op == "Put":
                        reply = self.servers[server_idx].call("KVServer.Put", args)
                    else:  # Append
                        reply = self.servers[server_idx].call("KVServer.Append", args)
                    
                    if reply is not None and hasattr(reply, "value"):
                        return reply.value if reply.value is not None else ""
                except (TimeoutError, Exception):
                    continue
            
            # Brief pause before retrying all servers
            time.sleep(0.001)

    def put(self, key: str, value: str):
        """Install or replace the value for a particular key"""
        self.put_append(key, value, "Put")

    def append(self, key: str, value: str) -> str:
        """Append value to key's value and return the old value"""
        return self.put_append(key, value, "Append")