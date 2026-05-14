import Pyro5.api
import Pyro5.errors
import time

def send_command_to_cluster(command):
    """
    Attempts to send a command to the Raft cluster leader.
    Implements a lookup loop with error handling to support 
    election periods where the leader might be temporarily absent.
    """
    while True:
        try:
            ns = Pyro5.api.locate_ns()
            leader_uri = ns.lookup("leader")
            
            # Cria um proxy para se comunicar com o nó líder
            with Pyro5.api.Proxy(leader_uri) as leader:
                success = leader.append_log(command)
                
                if success:
                    print(f"Success: Command '{command}' accepted by leader.")
                    return True
                else:
                    # Caso o nó encontrado não seja mais o líder (stale registration)
                    print("The node found is no longer the leader. Retrying...")
                    
        except Pyro5.errors.NamingError:
            # Ocorre durante eleição
            print("Warning: Leader not found in Name Server. Election in progress? Retrying in 1s...")
        except Pyro5.errors.CommunicationError:
            print("Communication error: Check if Name Server and nodes are active. Retrying...")
        except Exception as e:
            print(f"Unexpected error: {e}. Retrying...")
            
        # Espera um curto período antes da próxima tentativa
        time.sleep(1)

if __name__ == "__main__":
    print("--- Interactive Raft Client ---")
    try:
        while True:
            user_input = input("\nEnter command for the cluster (or 'exit' to quit): ")
            if user_input.lower() in ['exit', 'quit', 'sair']:
                break
            
            if user_input.strip():
                send_command_to_cluster(user_input)
    except KeyboardInterrupt:
        print("\nClosing client.")