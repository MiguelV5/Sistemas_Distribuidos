import csv
from enum import Enum
import io

class QueryMessageType(Enum):
    EOF_B = 1
    EOF_R = 2
    DATA = 3
    DATA_ACK = 4
    WAIT_FOR_SV = 5

class QueryMessage:
    def __init__(self, msg_type: Enum, client_id: int, payload: str = ""):
        self.msg_type = msg_type
        self.client_id = client_id
        self.payload = payload

    def encode_to_str(self) -> str:
        return f"{self.msg_type.value}|{self.client_id}|{self.payload}"
    
    @classmethod
    def decode_from_str(cls, msg: str):
        msg_type, client_id, payload = msg.split("|")
        return cls(QueryMessageType(int(msg_type)), int(client_id), payload)
    


# ========================================================================================================
 


class SystemMessageType(Enum):
    EOF_B = 1
    EOF_R = 2
    DATA = 3
    HEALTH_CHECK = 4
    ALIVE = 5

class SystemMessage:
    def __init__(self, msg_type: Enum, client_id: int = 0, worker_id: int = 1, payload: str = ""):
        self.msg_type = msg_type
        # defaults to 0 ONLY for health checks
        self.client_id = client_id
        # defaults to 1, as there are processes that are not replicated
        self.worker_id = worker_id
        self.payload = payload

    def encode_to_str(self) -> str:
        return f"{self.msg_type.value}|{self.client_id}|{self.worker_id}|{self.payload}"
    
    @classmethod
    def decode_from_bytes(cls, raw_msg_body: bytes):
        msg = raw_msg_body.decode()
        msg_type, client_id, worker_id, payload = msg.split("|")
        return cls(SystemMessageType(int(msg_type)), int(client_id), int(worker_id), payload)
    
    def get_batch_iter_from_payload(self):
        if self.msg_type == SystemMessageType.DATA:
            return csv.reader(io.StringIO(self.payload), delimiter=',', quotechar='"')
        else:
            return None
    
