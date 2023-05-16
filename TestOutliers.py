import HashTableClient
import sys
import time

if __name__=="__main__":
    try:
        client = HashTableClient.HashTableClient(project_name=sys.argv[1])
    except IndexError:
        print("Usage: python3 TestPerf.py <Server Name>")
        sys.exit(1)
        
    client.connect()
    
    insert_min = float('inf')
    insert_max = 0
    print("getting insert times")
    for i in range(5000):
        start_time = time.time_ns()
        client.insert(f'{i}', {'20': i})
        end_time = time.time_ns()
        insert_min = min(insert_min, end_time - start_time)
        insert_max = max(insert_max, end_time - start_time)
        
    remove_min = float('inf')
    remove_max = 0
    print("getting remove times")
    for i in range(5000):
        start_time = time.time_ns()
        client.remove(f'{i}')
        end_time = time.time_ns()
        remove_min = min(remove_min, end_time - start_time)
        remove_max = max(remove_max, end_time - start_time)
        
    print(f"insert min: {insert_min/1000000000}s\ninsert max: {insert_max/1000000000}s\n\nremove min: {remove_min/1000000000}s\nremove max: {remove_max/1000000000}s")