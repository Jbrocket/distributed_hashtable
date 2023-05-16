import HashTableClient
import random

class ClusterClient:
    def __init__(self, server_name, num_servers, k_copies):
        self._num_servers = num_servers
        self._k_copies = k_copies
        self._clients: list(HashTableClient) = [HashTableClient.HashTableClient(project_name=f"{server_name}-{i}") for i in range(num_servers)]
    
    def _hash(self, data):
        if isinstance(data, str):
            total = 0
            for char in data:
                total += ord(char)
            return (total % 7) % self.get_num_servers()
        elif isinstance(data, int):
            return (data % 7) % self.get_num_servers()
        return 0
        
    def connect(self):
        for client in self.get_client_list():
            client.connect()
        return
    
    def disconnect(self):
        for client in self.get_client_list():
            client.disconnect()
            
    
    def insert(self, key, value):
        index = self._hash(key)
        client_list = self.get_client_list()
        res = None
        for i in range(index, index + self.get_k_copies()):
            res = client_list[i%self.get_num_servers()].insert(key, value)
        return res
    
    def lookup(self, key):
        index = self._hash(key)
        client_list = self.get_client_list()
        random_index = random.randint(index, index + self.get_k_copies() - 1)
        result = client_list[random_index % self.get_num_servers()].lookup(key)
        
        while result == "server down":
            for i in range(index, index + self.get_k_copies()):
                res = client_list[i % self.get_num_servers()].lookup(key)
                if res != "server down":
                    result = res
                    break
        
        return result
    
    def remove(self, key):
        index = self._hash(key)
        client_list = self.get_client_list()
        res = None
        for i in range(index, index + self.get_k_copies()):
            res = client_list[i%self.get_num_servers()].remove(key)
        return res
    
    def query(self, subkey, subvalue):
        client_list = self.get_client_list()
        cur_set = set()
        total = []
        for client in client_list:
            result = client.query(subkey, subvalue)
            if result[:10] == "status 400":
                return result
            if isinstance(result, list):
                for tuple in result:
                    if tuple[0] not in cur_set:
                        cur_set.add(tuple[0])
                        total.append(tuple)
                    
        return total
    
    def size(self):
        total = 0
        for client in self.get_client_list():
            total += client.size()
        return int(total / self.get_k_copies())
    
    def get_num_servers(self):
        return self._num_servers
    
    def get_k_copies(self):
        return self._k_copies
    
    def get_client_list(self):
        return self._clients