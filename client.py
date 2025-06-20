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
        self.client_id = nrand()  # Unique client ID
        self.seq = 0  # Operation sequence number
        self.mu = threading.Lock()
        
        # Get sharding configuration
        self.nshards = getattr(cfg, "nservers", 1)

    def _shard_for_key(self, key):
        # Determine which shard a key belongs to
        try:
            return int(key) % self.nshards
        except:
            # For non-integer keys, use hash
            return hash(key) % self.nshards

    def get(self, key: str) -> str:
        # Increment sequence number atomically
        with self.mu:
            seq = self.seq
            self.seq += 1
            
        args = GetArgs(key, self.client_id, seq)
        
        while True:  # Keep trying until we get a response
            if self.nshards <= 1:
                # Single server mode: try all servers in round-robin
                for i in range(len(self.servers)):
                    try:
                        reply = self.servers[i].call("KVServer.Get", args)
                        if reply is not None and (not hasattr(reply, "err") or reply.err is None):
                            return reply.value if reply.value is not None else ""
                    except Exception:
                        continue
            else:
                # Sharded mode: start with the primary server for this key
                shard = self._shard_for_key(key)
                
                # Try each server in round-robin starting from the primary
                for _ in range(len(self.servers)):
                    try:
                        reply = self.servers[shard].call("KVServer.Get", args)
                        if reply is not None:
                            if not hasattr(reply, "err") or reply.err is None:
                                return reply.value if reply.value is not None else ""
                            elif reply.err != "ErrWrongGroup":
                                # For errors other than wrong group, retry with same server
                                continue
                    except Exception:
                        pass
                    
                    # Try next server
                    shard = (shard + 1) % len(self.servers)

    def put_append(self, key: str, value: str, op: str) -> str:
        # Increment sequence number atomically
        with self.mu:
            seq = self.seq
            self.seq += 1
            
        args = PutAppendArgs(key, value, self.client_id, seq)
        
        while True:  # Keep trying until we get a response
            if self.nshards <= 1:
                # Single server mode: try all servers in round-robin
                for i in range(len(self.servers)):
                    try:
                        reply = self.servers[i].call("KVServer." + op, args)
                        if reply is not None and (not hasattr(reply, "err") or reply.err is None):
                            return reply.value if hasattr(reply, "value") and reply.value is not None else ""
                    except Exception:
                        continue
            else:
                # Sharded mode: start with the primary server for this key
                shard = self._shard_for_key(key)
                
                # Try each server in round-robin starting from the primary
                for _ in range(len(self.servers)):
                    try:
                        reply = self.servers[shard].call("KVServer." + op, args)
                        if reply is not None:
                            if not hasattr(reply, "err") or reply.err is None:
                                return reply.value if hasattr(reply, "value") and reply.value is not None else ""
                            elif reply.err != "ErrWrongGroup":
                                # For errors other than wrong group, retry with same server
                                continue
                    except Exception:
                        pass
                    
                    # Try next server
                    shard = (shard + 1) % len(self.servers)

    def put(self, key: str, value: str):
        self.put_append(key, value, "Put")

    def append(self, key: str, value: str) -> str:
        return self.put_append(key, value, "Append")