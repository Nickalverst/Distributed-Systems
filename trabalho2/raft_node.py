from utils import RaftPort, NodeState, LogEntry
import Pyro5
import Pyro5.server
import Pyro5.api
import threading
import random
import time

class RaftNode:
    def __init__(self, node_name):
        if node_name not in RaftPort._member_names_:
            raise ValueError(f"Invalid node name: {node_name}. Must be one of: {RaftPort._member_names_}")
        
        # Pyro information
        self.node_name = node_name
        self.port = RaftPort[node_name].value
        self.uri = RaftPort[node_name].get_pyro_uri()
        self.state = NodeState.FOLLOWER
        self.pyro_daemon = Pyro5.server.Daemon(port=self.port)
        self.pyro_daemon.register(self, objectId=f"raft_{self.node_name}")
        self.other_nodes = [RaftPort[n].get_pyro_uri() for n in RaftPort._member_names_ if n != node_name]
        daemon_thread = threading.Thread(target=self.pyro_daemon.requestLoop, daemon=True)
        daemon_thread.start()

        # Persistent state on all servers:
        self.current_term = 0
        self.voted_for = None
        self.log: list[LogEntry] = []

        # Volatile state on all servers:
        self.commit_index = -1

        # Volatile state on leaders:
        self.next_index = {}
        self.match_index = {}

        self.lock = threading.Lock()

        self.timer = None
        self.__reset_election_timer()

    @Pyro5.api.expose
    def append_log(self, log):
        with self.lock:
            if self.state != NodeState.LEADER:
                print(f"WARNING: {self.node_name} is not leader. Command ignored.")
                return False

            # O log serve como o 'buffer' uncommitted até que o commit_index suba.
            new_entry = LogEntry(term=self.current_term, command=log)
            self.log.append(new_entry)

            print(f"Leader {self.node_name} . Saved uncommitted log with index {len(self.log)-1}.")

            #Confirmação imediata para o cliente, 
            # o commit real ocorrerá quando a maioria dos seguidores replicar a entrada
            return True

    def __reset_election_timer(self):
        if self.timer:
            self.timer.cancel()
        
        random_timeout = random.uniform(1, 3)
        
        self.timer = threading.Timer(random_timeout, self.__become_candidate)
        self.timer.start()

    def __become_candidate(self):
        print(f"Timeout of term {self.current_term}! Beginning election...")

        with self.lock:
            self.state = NodeState.CANDIDATE
            self.current_term += 1
            self.voted_for = self.node_name

        # Reinicia o timer antes de iniciar a eleição para verificar 
        # se recebe votos suficientes antes de outro timeout ocorrer
        self.__reset_election_timer() 

        if self.__start_election(self.current_term, self.node_name, len(self.log)):
            print("Election won!")
            self.__become_leader()
        else:
            print("Election lost.")

    def __start_election(self, term, candidate_id, candidate_log_length):
        """Método PyRO chamado por um candidato para solicitar voto."""
        #  Reply false if term < currentTerm (§5.1)
        if term < self.current_term:
            return False
        
        # If votedFor is null or candidateId, and candidate’s log is at
        # least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
        with self.lock:
            self.voted_for = self.node_name  
        votes_received = 1 

        for uri in self.other_nodes:
            try:
                with Pyro5.api.Proxy(uri) as node_proxy:
                    term_received, vote_granted = node_proxy.request_vote(term=self.current_term,
                                                                          candidate_id=self.node_name,
                                                                          last_log_index=len(self.log) - 1,
                                                                          last_log_term=self.log[-1].term if self.log else 0)
                    if term_received > self.current_term:
                        with self.lock:
                            self.current_term = term_received
                        return False
                    
                    if vote_granted:
                        votes_received += 1
            except Exception:
                print(f"Error contacting node in URI {uri}")
        print(f"Votes received: {votes_received} out of {RaftPort.__len__()}")
        return votes_received > (RaftPort.__len__() / 2)

    @Pyro5.api.expose
    def request_vote(self, term, candidate_id, last_log_index, last_log_term):
        # 1. Reply false if term < currentTerm (§5.1)
        if term < self.current_term:
            print(f"Node {self.node_name}: Vote denied to {candidate_id} because term {term} is less than current term {self.current_term}.")
            return self.current_term, False

        if term > self.current_term:
            with self.lock:
                self.current_term = term
                self.voted_for = None
                self.state = NodeState.FOLLOWER

        # 2. If votedFor is null or candidateId, and candidate’s log is at
        # least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
        if self.voted_for is None or self.voted_for == candidate_id:
            my_last_index = len(self.log) - 1
            my_last_term = self.log[-1].term if self.log else 0

            log_is_up_to_date = (
                last_log_term > my_last_term
                or (
                    last_log_term == my_last_term
                    and last_log_index >= my_last_index
                )
            )

            if log_is_up_to_date:
                self.voted_for = candidate_id
                self.__reset_election_timer()
                print(f"Node {self.node_name}: Vote granted to {candidate_id} for term {term}.")
                return self.current_term, True
            else:
                print(f"Node {self.node_name}: Vote denied to {candidate_id} because candidate's log is not up-to-date.")
        else:
            print(f"Node {self.node_name}: Vote denied to {candidate_id} because already voted for {self.voted_for}.")

        return self.current_term, False
        
    def __become_leader(self):
        with self.lock:
            self.state = NodeState.LEADER
        if self.timer:
            self.timer.cancel()

        last_log_index = len(self.log)

        for uri in self.other_nodes:
            self.next_index[uri] = last_log_index
            self.match_index[uri] = -1

        ns = Pyro5.api.locate_ns()
        ns.register("leader", self.uri)
        print(f"Node {self.node_name} is now the leader for term {self.current_term}!")

        heartbeat_thread = threading.Thread(
            target=self.__heartbeat_loop,
            daemon=True
        )
        heartbeat_thread.start()

    @Pyro5.api.expose
    def append_entries(self, term, leader_id, prev_log_index, prev_log_term, entries, leader_commit):
        with self.lock:
            # 1. Reply false if term < currentTerm (§5.1)
            if term < self.current_term:
                return self.current_term, False

            if term > self.current_term:
                self.current_term = term
                self.state = NodeState.FOLLOWER
                self.voted_for = None

            self.__reset_election_timer()

            if prev_log_index >= 0:
                # 2. Reply false if log doesn’t contain an entry at prevLogIndex
                # whose term matches prevLogTerm (§5.3)
                if prev_log_index >= len(self.log):
                    return self.current_term, False

                if self.log[prev_log_index].term != prev_log_term:
                    return self.current_term, False

            # 3. If an existing entry conflicts with a new one (same index
            # but different terms), delete the existing entry and all that
            # follow it (§5.3)
            insert_index = prev_log_index + 1

            for i, entry_data in enumerate(entries):
                entry = LogEntry.from_dict(entry_data)
                local_index = insert_index + i
                if local_index < len(self.log):
                    if self.log[local_index].term != entry.term:
                        self.log = self.log[:local_index]
                        break

            for i, entry_data in enumerate(entries):
                entry = LogEntry.from_dict(entry_data)
                local_index = insert_index + i
                if local_index >= len(self.log):
                    self.log.append(entry)

            # 5. If leaderCommit > commitIndex, set commitIndex =
            # min(leaderCommit, index of last new entry
            if leader_commit > self.commit_index:
                self.commit_index = min(
                    leader_commit,
                    len(self.log) - 1
                )

            return self.current_term, True

    def __heartbeat_loop(self):
        while self.state == NodeState.LEADER:
            for uri in self.other_nodes:
                try:
                    with self.lock:
                        # O líder rastreia qual o próximo índice enviar para cada seguidor 
                        next_i = self.next_index.get(uri, len(self.log))
                        prev_i = next_i - 1
                        prev_t = self.log[prev_i].term if prev_i >= 0 else 0

                        # Envia as entradas a partir do next_index para replicar o log 
                        entries_to_send = [entry.to_dict() for entry in self.log[next_i:]]
                        current_commit = self.commit_index

                    with Pyro5.api.Proxy(uri) as proxy:

                        proxy._pyroTimeout = 0.2
                        
                        term, success = proxy.append_entries(
                            self.current_term,
                            self.node_name,
                            prev_i,
                            prev_t,
                            entries_to_send,
                            current_commit
                        )

                        # Se o seguidor tiver um termo maior, o líder deve renunciar 
                        if term > self.current_term:
                            with self.lock:
                                self.current_term = term
                                self.state = NodeState.FOLLOWER
                            return

                        if success:
                            # Se replicado com sucesso, atualiza o progresso do seguidor 
                            self.match_index[uri] = prev_i + len(entries_to_send)
                            self.next_index[uri] = self.match_index[uri] + 1
                        else:
                            # Se falhar por inconsistência, retrocede um índice e tenta de novo 
                            self.next_index[uri] = max(0, self.next_index[uri] - 1)

                except Exception:
                    pass
                    #print(f"Node {uri} unreachable")

            # Lógica de Commit por Maioria:
            # Verifica se existe um índice N > commit_index que está na maioria dos seguidores 
            with self.lock:
                for n in range(len(self.log) - 1, self.commit_index, -1):
                    count = 1 
                    for m_idx in self.match_index.values():
                        if m_idx >= n:
                            count += 1

                    # Se a maioria confirmou e a entrada é do termo atual, efetiva o commit
                    if count > (len(RaftPort) / 2) and self.log[n].term == self.current_term:
                        self.commit_index = n
                        print(f"Log COMMITTED up to index {n} by the majority!")
                        break

            time.sleep(0.5) 
            
def main():
    raft_node = None

    while raft_node is None:
        try:
            node_name = input(f"Enter the Raft node name {RaftPort._member_names_}: ")
            raft_node = RaftNode(node_name)
        except ValueError as e:
            print(e)
            raft_node = None
        except KeyboardInterrupt:
            print("\nExiting.")
            return

    print(f"Starting Raft node '{node_name}' on port {raft_node.port} with URI {raft_node.uri}")

    while True:
        time.sleep(1)

if __name__ == "__main__":
    main()