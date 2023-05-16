import socket
import sys
import json
import collections
import http.client
import time

class HashTableClient:
    def __init__(self, host=None, port=None, project_name=None):
        self.host = host
        self.port = port
        self.project_name = project_name
        self.socket = None
        
    def disconnect(self):
        self.socket.close()
        self.socket = None    
    
    def find_host(self):
        s = http.client.HTTPConnection("catalog.cse.nd.edu:9097")
        s.request('GET', "/query.json")
        server_list = s.getresponse()
        server_list = json.loads(server_list.read())
        
        my_name = None
        for server in server_list:
            try:
                if my_name and server['project'] == self.project_name:
                    if server['lastheardfrom'] > my_name[0]:
                        my_name = (server['lastheardfrom'] , server['project'], server['port'], server['name'])
                elif server['project'] == self.project_name:
                    my_name = (server['lastheardfrom'] , server['project'], server['port'], server['name'])
            except KeyError:
                continue
        
        if not my_name:
            return
            
        # print(my_name)
        self.host = my_name[3]
        self.port = my_name[2]
        return
    
    ### Connect to the server
    def connect(self, json={}):
        skip = False
        if json.get("method", None) == "lookup" or json.get("method", None) == "query" or json.get("method", None) == "size":
            skip = True
        if not self.port or not self.host:
            self.find_host()

        if self.socket:
            self.socket.close()
            
        self.socket = socket.socket()
        
        while(True):
            try:
                self.socket.connect((self.host, self.port))
                print("Connected")
                break
            except (ConnectionRefusedError, TypeError) as e:
                if not self.project_name:
                    print("Usage: python3 HashTableClient.py <Server Name>")
                    sys.exit(1)
                if skip:
                    return "server down"
            time.sleep(5)
            self.find_host()
            
    ### Send a singular JSON request to the server
    def _send_request(self, json={}, simple=True):
        server_response = []
        
        if not self.socket:
            if self.connect(json=json) == "server down":
                return "server down"
            
        response = self.send_json(req_json=json, server_responses=server_response)
        if response == "server down":
            return "server down"
            
        if simple:
            server_response = self.simple(server_response)
            
        try:
            return server_response[0]
        except IndexError:
            return "status 404: Server not found"
    
    ### Send a list of requests to the server
    def _batch_requests(self, json_list=[], simple=True):
        server_responses = []

        if not self.socket:
            self.connect()
            
        while True:
            if json_list:
                for python_dict in json_list:
                    self.send_json(req_json=python_dict, server_responses=server_responses)
                break
            
        if simple:
            server_responses = self.simple(server_responses)
        return server_responses
    
    ########################
    ## METHODS FOR CLIENT ##
    ########################
    
    def insert(self, key, value):
        if type(key) == type({}) or type(key) == type([]) or type(key) == type(int(0)) or type(key) == type(set([1])):
            args = {'message': 'Type Error for key, not a hashable type, use string instead'}
            return self._send_request(self.error_json(args))
        if type(value) == type({}):
            stack = [value]
            while stack:
                cur = stack.pop()
                for keys in cur:
                    if type(keys) != type(str("string :)")):
                        args = {'message': 'Type Error for subkeys, not a hashable type, use string instead'}
                        return self._send_request(self.error_json(args))
                    if type(cur[keys]) == type({}):
                        stack.append(cur[keys])
        return self._send_request(self.create_json('insert', [('status', 200),('key', key), ('value', value)]))
    
    def remove(self, key):
        if type(key) == type({}) or type(key) == type([]) or type(key) == type(int(0)) or type(key) == type(set([1])):
            args = {'message': 'Type Error for key, not a hashable type, use string instead'}
            return self._send_request(self.error_json(args))
        return self._send_request(self.create_json('remove', [('status', 200),('key', key)]))
    
    def lookup(self, key):
        if type(key) == type({}) or type(key) == type([]) or type(key) == type(int(0)) or type(key) == type(set([1])):
            args = {'message': 'Type Error for key, not a hashable type, use string instead'}
            return self._send_request(self.error_json(args))
        return self._send_request(self.create_json('lookup', [('status', 200),('key', key)]))
    
    def query(self, subkey, subvalue):
        if type(subkey) == type({}) or type(subkey) == type([]) or type(subkey) == type(int(0)) or type(subkey) == type(set([1])):
            args = {'message': 'Type Error for subkey, not a hashable type, use string instead'}
            return self._send_request(self.error_json(args))
        return self._send_request(self.create_json('query', [('status', 200),('subkey', subkey), ('subvalue', subvalue)]))
    
    def size(self):
        return self._send_request(self.create_json('size', [('status', 200)]))
    
    ########################
    ## ################## ##
    ########################

    def error_json(self, args=collections.defaultdict(lambda: None)):
        return {'header': {'status': 400, 'message': 'BadRequest'}, 'data' : {'result': {'error': args['message']}}, 'status': 400}

    ### Create Json outlined in client methods
    def create_json(self, method, args):
        json = {'method': method}
        for arg in args:
            json[arg[0]] = arg[1]
        return json
    
    ### Send the JSON to the server and get response
    def send_json(self, req_json, server_responses):
        if req_json['status'] != 400:
            skip = None
            rec = ""
            try:
                serv_request = json.dumps(req_json)
                self.socket.sendall(f"{len(serv_request.encode('utf-8')):>16}{serv_request}".encode('utf-8'))
                
                size = self.socket.recv(16)
                
            except (ConnectionResetError, socket.timeout) as e:
                skip = 1
                if self.connect(json=req_json) == "server down":
                    return "server down"
                
            
            if not skip:
                try:
                    size = int(size.decode())
                except ValueError:
                    size = -1
                    pass
                while size > len(rec):
                    get_bytes = size - len(rec) if size-len(rec) > 1024 else 1024
                    data = self.socket.recv(get_bytes)
                    if not data:
                        break
                    rec += data.decode()
                
                try:
                    server_responses.append(json.loads(rec))
                except:
                    if self.connect(json=req_json) == "server down":
                        return "server down"
                    self.send_json(req_json, server_responses)
        else:
            server_responses.append(req_json)
            
    ### Simplify the output so the user just sees if it was successful or not
    def simple(self, server_response):
        res = []
        for response in server_response:
            if response['header']['status'] == 200:
                if response['data']['requestData']['request'] == 'insert':
                    res.append(response['data']['result']['value'])
                elif response['data']['requestData']['request'] == 'remove':
                    res.append(response['data']['result']['value'])
                elif response['data']['requestData']['request'] == 'lookup':
                    res.append(response['data']['result']['value'])
                elif response['data']['requestData']['request'] == 'query':
                    res.append(response['data']['result']['value'])
                elif response['data']['requestData']['request'] == 'size':
                    res.append(response['data']['result']['value'])
            else:
                res.append(f"status {response['header']['status']}: {response['header']['message']}")
                try:
                    res[-1] += f" -- {response['data']['result']['error']}"
                except KeyError:
                    pass
        return res

if __name__=="__main__":
    # try:
    #     Client = HashTableClient(sys.argv[1], int(sys.argv[2]))
    # except ValueError:
    #     print("Usage: python3 HashTableClient.py <Host Name> <Port Number>")
    #     sys.exit(1)
        
    try:
        if isinstance(sys.argv[1], str):
            pass
        else:
            exit(1)
    except IndexError:
        print("Usage: python3 HashTableClient.py <Server Name>")
        exit(1)
        
        
    Client = HashTableClient(project_name=sys.argv[1])
    
    Client.connect()

    try:
        while True:
            print(Client.insert('10', 20))
            print(Client.insert('11', 30))
            print(Client.remove('10'))
            print(Client.size())
            print(Client.lookup('11'))
            print(Client.lookup('10'))
            print(Client.insert('10', {'10': 20}))
            print(Client.insert('11', {10: 20}))
            print(Client.size())
            print(Client.query('10', 20))
            print(Client.remove('10'))
            print(Client.remove('11'))
    except KeyboardInterrupt:
        print("Client killed by an interruption ( I hope you're happy (ㅠ﹏ㅠ) ).")
        pass
    Client.disconnect()