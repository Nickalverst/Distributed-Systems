from enum import Enum
from dataclasses import dataclass

class RaftPort(Enum):
    node1 = 9001
    node2 = 9002
    node3 = 9003    
    node4 = 9004

    def get_pyro_uri(self):
        return f"PYRO:raft_{self.name}@localhost:{self.value}"

class NodeState(Enum):
    FOLLOWER = 1
    CANDIDATE = 2
    LEADER = 3

@dataclass
class LogEntry:
    def __init__(self, term, command):
        self.term = term
        self.command = command

    def to_dict(self):
        return {"term": self.term, "command": self.command}

    @staticmethod
    def from_dict(data):
        return LogEntry(term=data["term"], command=data["command"])