import Pyro5.api
from fortune import fortune
import datetime

name = 'leader'

if __name__ == "__main__":
    locate_ns = Pyro5.api.locate_ns()
    
    while True:
        uri = locate_ns.lookup(name)
        input("Press enter to send log to the leader...")
        log = fortune()
        log = f"[{datetime.datetime.now()}] {log}"
        print(f"Sending log: {log}")
        with Pyro5.api.Proxy(uri) as leader:
            leader.append_log(log)
