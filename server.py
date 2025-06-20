import logging
import threading
from typing import Tuple, Any

debugging = False

def debug(format, *args):
    if debugging:
        logging.info(format % args)

# Put or Append
class PutAppendArgs:
    def __init__(self, key, value, client_id=None, seq=None):
        self.key = key
        self.value = value
        self.client_id = client_id
        self.seq = seq

class PutAppendReply:
    def __init__(self, value=None, err=None):
        self.value = value
        self.err = err

class GetArgs:
    def __init__(self, key, client_id=None, seq=None):
        self.key = key
        self.client_id = client_id
        self.seq = seq

class GetReply:
    def __init__(self, value=None, err=None):
        self.value = value
        self.err = err

class KVServer:
    def __init__(self, cfg):
        self.mu = threading.Lock()
        self.cfg = cfg
        self.kv = {}  # key-value store
        self.client_last = {}  # {client_id: (last_seq, last_reply)}
        
        # Sharding configuration
        self.nshards = getattr(cfg, "nservers", 1)
        self.server_id = None
        if hasattr(cfg, "kvservers"):
            for i, s in enumerate(cfg.kvservers):
                if s is self:
                    self.server_id = i
                    break

    def _owns_key(self, key):
        # For single server setups or if we couldn't identify our server_id
        if self.nshards <= 1 or self.server_id is None:
            return True
            
        # Calculate the shard for this key
        try:
            shard_id = int(key) % self.nshards
        except:
            # For non-integer keys, use hash
            shard_id = hash(key) % self.nshards
            
        # We own this key if the shard_id matches our server_id
        return shard_id == self.server_id

    def Get(self, args: GetArgs):
        with self.mu:
            # Check if we own this key
            if not self._owns_key(args.key):
                return GetReply(value="", err="ErrWrongGroup")
                
            # Check for duplicate request
            if args.client_id is not None and args.seq is not None:
                last = self.client_last.get(args.client_id)
                if last and last[0] == args.seq:
                    return last[1]  # Return cached response
                    
            # Process the request
            value = self.kv.get(args.key, "")
            reply = GetReply(value=value)
            
            # Cache the response
            if args.client_id is not None and args.seq is not None:
                self.client_last[args.client_id] = (args.seq, reply)
                
            return reply

    def Put(self, args: PutAppendArgs):
        with self.mu:
            # Check if we own this key
            if not self._owns_key(args.key):
                return PutAppendReply(err="ErrWrongGroup")
                
            # Check for duplicate request
            if args.client_id is not None and args.seq is not None:
                last = self.client_last.get(args.client_id)
                if last and last[0] == args.seq:
                    return last[1]  # Return cached response
                    
            # Process the request
            self.kv[args.key] = args.value
            reply = PutAppendReply()
            
            # Cache the response
            if args.client_id is not None and args.seq is not None:
                self.client_last[args.client_id] = (args.seq, reply)
                
            return reply

    def Append(self, args: PutAppendArgs):
        with self.mu:
            # Check if we own this key
            if not self._owns_key(args.key):
                return PutAppendReply(err="ErrWrongGroup")
                
            # Check for duplicate request
            if args.client_id is not None and args.seq is not None:
                last = self.client_last.get(args.client_id)
                if last and last[0] == args.seq:
                    return last[1]  # Return cached response
                    
            # Process the request
            old_value = self.kv.get(args.key, "")
            self.kv[args.key] = old_value + args.value
            reply = PutAppendReply(value=old_value)
            
            # Cache the response
            if args.client_id is not None and args.seq is not None:
                self.client_last[args.client_id] = (args.seq, reply)
                
            return reply