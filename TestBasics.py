import sys
import ClusterClient

if __name__=="__main__":
    try:
        client = ClusterClient.ClusterClient(server_name=sys.argv[1], num_servers=int(sys.argv[2]), k_copies=int(sys.argv[3]))
        print(len(sys.argv))
    except (IndexError, ValueError, TypeError) as e:
        print("Usage: python3 TestBasics.py <Server Name> <num_servers> <k_copies>")
        sys.exit(1)
        
    client.connect()
    
    
    print(client.insert('10', 20)) # True
    print(client.insert('11', 30)) # True
    print(client.remove('10')) # True
    print(client.remove('9')) # True 
    print(client.size()) # 1
    print(client.lookup('11')) # 30
    print(client.lookup('10')) # Error - no key
    print(client.insert('10', {'10': 20})) # True
    print(client.insert('11', {10: 20})) # Key error since it has to be string
    print(client.insert('11', {'10': 20, '20': {30: 10}})) # Key error since it has to be string
    print(client.insert('11', {'10': 20, '20': {'30': 10}})) # True
    print(client.lookup('11')) # {'10': 20, '20': {'30': 10}}
    print(client.insert('1', 20)) # True
    print(client.insert('2', 20)) # True
    print(client.insert('3', 20)) # True
    
    ## I MEANT TYPE ERROR, I'M NOT RETYPING
    print(client.insert(10, 20)) # Value Error
    print(client.insert(['asdas'], 20)) # Value Error
    print(client.insert({'asdas': 10}, 20)) # Value Error
    print(client.query(10, 20)) # Value Error
    print(client.query(['asd'], 20)) # Value Error
    print(client.query({'asfa': 10}, 20)) # Value Error
    print(client.query({'asfa'}, 20)) # Value Error
    print(client.query(10, 20)) # Value Error
    print(client.lookup(10)) # Value Error
    print(client.lookup(['asdsa'])) # Value Error
    print(client.lookup({'asdsa': 10})) # Value Error
    print(client.remove(10)) # Value Error
    print(client.remove(['asdsa'])) # Value Error
    print(client.remove({'asdsa': 10})) # Value Error
    print(client.remove('11'))
    print(client.remove('10'))
    print(client.remove('1'))  #### These are to clear the table if needed
    print(client.remove('2'))
    print(client.remove('3'))
    print(client.size()) # 5
    print(client.query('10', 20)) # [['11', {'10': 20, '20': {'30': 10}}], ['10', {'10': 20}]]
    print(client.query('10', 21)) # []
    
    client.disconnect()