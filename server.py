import logging
import threading
from typing import Tuple, Any

debugging = False

def debug(format, *args):
    if debugging:
        logging.info(format % args)

class PutAppendArgs:
    def __init__(self, key, value):
        self.key = key
        self.value = value
        self.client_id = None
        self.seq_num = None
        self.op = None

class PutAppendReply:
    def __init__(self, value):
        self.value = value

class GetArgs:
    def __init__(self, key):
        self.key = key
        self.client_id = None
        self.seq_num = None

class GetReply:
    def __init__(self, value):
        self.value = value

class KVServer:
    def __init__(self, cfg):
        self.mu = threading.Lock()
        self.cfg = cfg
        self.kv = {}  # key -> value mapping
        self.last_ops = {}  # client_id -> (seq_num, result)
        self.nservers = getattr(cfg, "nservers", 1)
        self.nreplicas = getattr(cfg, "nreplicas", 1)
        self.server_id = self._find_server_id()

    def _find_server_id(self):
        """Find this server's ID in the configuration"""
        if hasattr(self.cfg, "kvservers") and self.cfg.kvservers:
            for i, server in enumerate(self.cfg.kvservers):
                if server is self:
                    return i
        return 0  # Default to 0 for single server case

    def _responsible_for_key(self, key):
        """Check if this server should handle the given key"""
        if self.nreplicas == 1:
            return True  # Single server case - handle all keys
        
        try:
            shard = int(key) % self.nservers
        except ValueError:
            shard = hash(key) % self.nservers
        
        # Check if this server is one of the replicas for this shard
        for i in range(self.nreplicas):
            responsible_server = (shard + i) % self.nservers
            if responsible_server == self.server_id:
                return True
        
        return False

    def _is_duplicate(self, client_id, seq_num):
        """Check if this request is a duplicate"""
        if client_id in self.last_ops:
            last_seq, last_result = self.last_ops[client_id]
            if seq_num == last_seq:
                return True, last_result
        return False, None

    def _record_request(self, client_id, seq_num, result):
        """Record the result of a request for duplicate detection"""
        self.last_ops[client_id] = (seq_num, result)

    def Get(self, args: GetArgs):
        with self.mu:
            # Check if this server should handle this key
            if not self._responsible_for_key(args.key):
                return GetReply("")  # Return empty for keys we don't handle
            
            # Check for duplicate request
            is_dup, cached_result = self._is_duplicate(args.client_id, args.seq_num)
            if is_dup:
                return cached_result
            
            # Get the value
            value = self.kv.get(args.key, "")
            reply = GetReply(value)
            
            # Record this request
            self._record_request(args.client_id, args.seq_num, reply)
            
            return reply

    def Put(self, args: PutAppendArgs):
        with self.mu:
            # Check if this server should handle this key
            if not self._responsible_for_key(args.key):
                return PutAppendReply("")  # Return empty for keys we don't handle
            
            # Check for duplicate request
            is_dup, cached_result = self._is_duplicate(args.client_id, args.seq_num)
            if is_dup:
                return cached_result
            
            # Put the value
            self.kv[args.key] = args.value
            reply = PutAppendReply("")
            
            # Record this request
            self._record_request(args.client_id, args.seq_num, reply)
            
            return reply

    def Append(self, args: PutAppendArgs):
        with self.mu:
            # Check if this server should handle this key
            if not self._responsible_for_key(args.key):
                return PutAppendReply("")  # Return empty for keys we don't handle
            
            # Check for duplicate request
            is_dup, cached_result = self._is_duplicate(args.client_id, args.seq_num)
            if is_dup:
                return cached_result
            
            # Get old value and append new value
            old_value = self.kv.get(args.key, "")
            self.kv[args.key] = old_value + args.value
            reply = PutAppendReply(old_value)  # Return the old value
            
            # Record this request
            self._record_request(args.client_id, args.seq_num, reply)
            
            return reply