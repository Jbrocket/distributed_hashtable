class HashTable:
    def __init__(self):
        self.table = {}
        
        
    def insert(self, key, value):
        self.table[key] = value
        return True
    
    
    def remove(self, key):
        try:
            self.table.pop(key)
        except KeyError:
            pass
        return True
    
    
    def lookup(self, key):
        try:
            return (True, self.table[key])
        except KeyError:
            return False
    
    
    def size(self):
        return len(self.table)
    
    
    def query(self, subkey, subvalue):
        results = []
        for key in self.table:
            if type(self.table[key]) == type({}):
                try:
                    if self.table[key][subkey] == subvalue:
                        results.append((key, self.table[key]))
                except KeyError:
                    pass
        return results
    
    def get_table(self):
        return self.table
    
    def set_table(self, table: dict):
        self.table = table
        return None
    
if __name__ == "__main__":
    hashtable = HashTable()
    print("-------INSERTS------\n")
    print(hashtable.insert(5, {"type": 4}))
    print(hashtable.insert(7, {"type": 4}))
    print(hashtable.insert(7, {"type": 3}))
    print(hashtable.insert(7, {"type": 3}))
    print(hashtable.insert(7, {"type": 3}))
    print(hashtable.insert(7, {"type": 3}))
    print(hashtable.insert(7, {"type": 3}))
    print(hashtable.insert(5, {"type": 4}))
    print(hashtable.insert(5, {"type": 4}))
    print(hashtable.insert(5, {"type": 4}))
    print("-------LOOKUPS------\n")
    print(hashtable.lookup(5))
    print(hashtable.lookup(3))
    print("-------REMOVES------\n")
    print(hashtable.remove(7))
    print(hashtable.remove(7))
    print(hashtable.remove(7))
    print(hashtable.remove(7))
    print(hashtable.remove(7))
    print(hashtable.insert(4, "hello"))
    print(hashtable.insert(5, {"name": "Jason"}))
    print(hashtable.query("type", 4))
    print(hashtable.query("nah", "false"))