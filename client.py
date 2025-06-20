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
        self.request_id = 0  # Monotonically increasing request ID
        self.mu = threading.Lock()

    def next_request_id(self):
        """Get next unique request ID"""
        with self.mu:
            self.request_id += 1
            return self.request_id

    def get_shard_servers(self, key):
        """Get list of servers responsible for this key"""
        if not hasattr(self.cfg, 'nreplicas') or self.cfg.nreplicas == 1:
            return list(range(len(self.servers)))
        
        nshards = len(self.servers) // self.cfg.nreplicas
        if nshards <= 1:
            return list(range(len(self.servers)))
        
        shard = int(key) % nshards
        start_server = shard * self.cfg.nreplicas
        end_server = start_server + self.cfg.nreplicas
        
        return list(range(start_server, min(end_server, len(self.servers))))

    def get(self, key: str) -> str:
        """Fetch the current value for a key. Returns "" if the key does not exist."""
        request_id = self.next_request_id()
        args = GetArgs(key, self.client_id, request_id)
        
        # Get servers responsible for this key
        shard_servers = self.get_shard_servers(key)
        
        # Try each server in the shard until we get a response
        while True:
            for server_id in shard_servers:
                try:
                    reply = self.servers[server_id].call("KVServer.Get", args)
                    if hasattr(reply, 'err') and reply.err == "WrongGroup":
                        continue  # Try next server
                    return reply.value if reply.value is not None else ""
                except:
                    # Network error, try next server
                    continue
            
            # If we get here, all servers failed, wait a bit and retry
            time.sleep(0.01)

    def put_append(self, key: str, value: str, op: str) -> str:
        """Shared by Put and Append operations"""
        request_id = self.next_request_id()
        args = PutAppendArgs(key, value, op, self.client_id, request_id)
        
        # Get servers responsible for this key
        shard_servers = self.get_shard_servers(key)
        
        # For linearizability in sharded case, always try primary server first
        if hasattr(self.cfg, 'nreplicas') and self.cfg.nreplicas > 1:
            # Sort so primary server (first in shard) is tried first
            shard_servers = sorted(shard_servers)
        
        # Try each server in the shard until we get a response
        while True:
            for server_id in shard_servers:
                try:
                    if op == "Put":
                        reply = self.servers[server_id].call("KVServer.Put", args)
                    else:  # Append
                        reply = self.servers[server_id].call("KVServer.Append", args)
                    
                    if hasattr(reply, 'err') and reply.err == "WrongGroup":
                        continue  # Try next server
                    
                    return reply.value if reply.value is not None else ""
                except:
                    # Network error, try next server
                    continue
            
            # If we get here, all servers failed, wait a bit and retry
            time.sleep(0.01)

    def put(self, key: str, value: str):
        """Install or replace value for a key"""
        self.put_append(key, value, "Put")

    def append(self, key: str, value: str) -> str:
        """Append value to key's value and return the old value"""
        return self.put_append(key, value, "Append")