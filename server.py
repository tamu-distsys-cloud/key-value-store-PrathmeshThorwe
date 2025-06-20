import logging
import threading

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
        self.kv = dict()
        self.last_ops = dict()  # client_id -> (seq_num, result)

    def _responsible_for_key(self, key):
        try:
            nshards = getattr(self.cfg, "nservers", 1)
            nreplicas = getattr(self.cfg, "nreplicas", 1)
            shard = int(key) % nshards
            my_indices = [i for i, s in enumerate(getattr(self.cfg, "kvservers", [])) if s is self]
            if not my_indices:
                return False
            my_index = my_indices[0]
            # Only the primary (first replica) for the shard processes requests
            return my_index == shard
        except Exception:
            return True

    def Get(self, args: GetArgs):
        with self.mu:
            if not self._responsible_for_key(args.key):
                return GetReply("")
            value = self.kv.get(args.key, "")
            reply = GetReply(value)
            return reply

    def Put(self, args: PutAppendArgs):
        with self.mu:
            if not self._responsible_for_key(args.key):
                return PutAppendReply("")
            cid, seq = args.client_id, args.seq_num
            if cid in self.last_ops and self.last_ops[cid][0] == seq:
                return PutAppendReply(self.last_ops[cid][1])
            self.kv[args.key] = args.value
            self.last_ops[cid] = (seq, args.value)
            return PutAppendReply(args.value)

    def Append(self, args: PutAppendArgs):
        with self.mu:
            if not self._responsible_for_key(args.key):
                return PutAppendReply("")
            cid, seq = args.client_id, args.seq_num
            if cid in self.last_ops and self.last_ops[cid][0] == seq:
                return PutAppendReply(self.last_ops[cid][1])
            old = self.kv.get(args.key, "")
            new = old + args.value
            self.kv[args.key] = new
            self.last_ops[cid] = (seq, old)
            reply = PutAppendReply(old)  # Return the old value, not the new!
            return reply
