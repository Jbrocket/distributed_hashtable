import ClusterClient
import sys
import time
import os

if __name__=="__main__":
    try:
        client = ClusterClient.ClusterClient(server_name=sys.argv[1], num_servers=int(sys.argv[2]), k_copies=int(sys.argv[3]))
    except (IndexError, ValueError, TypeError) as e:
        print("Usage: python3 TestBasics.py <Server Name> <num_servers> <k_copies>")
        sys.exit(1)
        
    client.connect()
    
    start_time = time.time_ns()
    for i in range(200):
        client.insert(key=f'{i}', value={f"{i%20}": 10})
    for i in range(200, 400):
        client.insert(key=f'{i}', value=10)
    end_time = time.time_ns()
    print(f"Insert|{(end_time - start_time)/1000000000}|{400/((end_time - start_time)/1000000000)}|{((end_time - start_time)/1000000000)/400}")
    
    start_time = time.time_ns()
    for i in range(400):
        client.lookup(f'{i}')
    end_time = time.time_ns()
    print(f"Look up|{(end_time - start_time)/1000000000}|{400/((end_time - start_time)/1000000000)}|{((end_time - start_time)/1000000000)/400}")

    
    start_time = time.time_ns()
    for i in range(400):
        client.query(f"{i%20}", 10)
    end_time = time.time_ns()
    print(f"Query|{(end_time - start_time)/1000000000}|{400/((end_time - start_time)/1000000000)}|{((end_time - start_time)/1000000000)/400}")
    

    start_time = time.time_ns()
    for i in range(400):
        client.remove(f"{i}")
    end_time = time.time_ns()
    print(f"Remove|{(end_time - start_time)/1000000000}|{400/((end_time - start_time)/1000000000)}|{((end_time - start_time)/1000000000)/400}")
    
    client.disconnect()