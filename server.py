import logging
import threading
from typing import Tuple, Any

debugging = False

# Use this function for debugging
def debug(format, *args):
    if debugging:
        logging.info(format % args)

# Put or Append
class PutAppendArgs:
    def __init__(self, key, value, op="", client_id=0, request_id=0):
        self.key = key
        self.value = value
        self.op = op  # "Put" or "Append"
        self.client_id = client_id
        self.request_id = request_id

class PutAppendReply:
    def __init__(self, value="", err=""):
        self.value = value  # For Append, this is the old value
        self.err = err

class GetArgs:
    def __init__(self, key, client_id=0, request_id=0):
        self.key = key
        self.client_id = client_id
        self.request_id = request_id

class GetReply:
    def __init__(self, value="", err=""):
        self.value = value
        self.err = err

class KVServer:
    def __init__(self, cfg):
        self.mu = threading.Lock()
        self.cfg = cfg
        self.data = {}  # key -> value mapping
        self.last_applied = {}  # client_id -> last applied request_id
        self.request_cache = {}  # (client_id, request_id) -> result
        
    def is_key_in_shard(self, key):
        """Check if this server should handle the given key"""
        if not hasattr(self.cfg, 'nreplicas') or self.cfg.nreplicas == 1:
            return True  # Single server case
            
        # For sharding, determine if this server handles this key
        nshards = self.cfg.nservers // self.cfg.nreplicas
        if nshards <= 1:
            return True
            
        shard = int(key) % nshards
        server_id = None
        
        # Find this server's ID
        for i, server in enumerate(self.cfg.kvservers):
            if server == self:
                server_id = i
                break
                
        if server_id is None:
            return False
            
        # Check if this server is responsible for this shard
        start_server = shard * self.cfg.nreplicas
        end_server = start_server + self.cfg.nreplicas
        
        return start_server <= server_id < end_server
    
    def is_duplicate(self, client_id, request_id):
        """Check if this request is a duplicate"""
        if (client_id, request_id) in self.request_cache:
            return True, self.request_cache[(client_id, request_id)]
        return False, None
    
    def record_request(self, client_id, request_id, result):
        """Record the result of a request"""
        # Clean up old requests to save memory
        if client_id in self.last_applied:
            old_request_id = self.last_applied[client_id]
            if (client_id, old_request_id) in self.request_cache:
                del self.request_cache[(client_id, old_request_id)]
        
        self.last_applied[client_id] = request_id
        self.request_cache[(client_id, request_id)] = result

    def Get(self, args: GetArgs):
        with self.mu:
            # Check if this server should handle this key
            if not self.is_key_in_shard(args.key):
                return GetReply("", "WrongGroup")
            
            # Check for duplicate
            is_dup, cached_result = self.is_duplicate(args.client_id, args.request_id)
            if is_dup:
                return cached_result
            
            # Get the value
            value = self.data.get(args.key, "")
            reply = GetReply(value, "")
            
            # Cache the result
            self.record_request(args.client_id, args.request_id, reply)
            
            return reply

    def Put(self, args: PutAppendArgs):
        with self.mu:
            # Check if this server should handle this key
            if not self.is_key_in_shard(args.key):
                return PutAppendReply("", "WrongGroup")
            
            # Check for duplicate
            is_dup, cached_result = self.is_duplicate(args.client_id, args.request_id)
            if is_dup:
                return cached_result
            
            # Put the value
            self.data[args.key] = args.value
            reply = PutAppendReply("", "")
            
            # Cache the result
            self.record_request(args.client_id, args.request_id, reply)
            
            return reply

    def Append(self, args: PutAppendArgs):
        with self.mu:
            # Check if this server should handle this key
            if not self.is_key_in_shard(args.key):
                return PutAppendReply("", "WrongGroup")
            
            # Check for duplicate
            is_dup, cached_result = self.is_duplicate(args.client_id, args.request_id)
            if is_dup:
                return cached_result
            
            # Get old value and append
            old_value = self.data.get(args.key, "")
            self.data[args.key] = old_value + args.value
            reply = PutAppendReply(old_value, "")
            
            # Cache the result
            self.record_request(args.client_id, args.request_id, reply)
            
            return reply