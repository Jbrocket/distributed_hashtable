import socket
import sys
import HashTable
import json
import os
import time
from threading import Thread
import select

# def WorkerThreads(Thread):
    

class HashTableServer:
    def __init__(self, host="", port=0, server_name=None):
        self.host = host
        self.port = port
        self.socket_set = set()
        self.master = None
        self.hashtable = HashTable.HashTable()
        self.valid_methods = set(['insert', 'remove', 'lookup', 'query','size'])
        self.transactionFile = None
        self.num_transactions = 0
        self.server_name = server_name
        
    def send_server_name(self):
        UDP_IP = "catalog.cse.nd.edu"
        UDP_PORT = int("9097")
        
        jsonMessage = {
                "type" : "hashtable",
                "owner" : "jbrocket",
                "port" : self.port,
                "project" : self.server_name
                }
        
        x = None
        
        try:
            while True:
                try:
                    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                    sock.sendto(json.dumps(jsonMessage).encode('utf-8'), (UDP_IP, UDP_PORT))
                except ConnectionRefusedError:
                    pass
                time.sleep(60)
                if x: # Fix syntax of vscode lol
                    break
        except KeyboardInterrupt:
                exit(1)
        return
    
    def create_response(self, request, conn, transaction=False):
        status = {'status': 400, 'message': 'BadRequest'}
        value = {'failedRequest': True}
        try:
            if request['method'] in self.valid_methods:
                if request['method'] == "insert":
                    value = self.hashtable.insert(request['key'], request['value'])
                    status, value = {'status': 200, 'message': 'Success'}, {'requestData': {'request': 'insert', 'key': request['key'], 'value': request['value']}, 'result': {'value': True}}
                    if not transaction:
                        self.transaction(json.dumps(request))
                elif request['method'] == "remove":
                    value = self.hashtable.remove(request['key'])
                    status, value = {'status': 200, 'message': 'Success'}, {'requestData': {'request': 'remove', 'key': request['key']}, 'result': {'value': True}}
                    if not transaction:
                        self.transaction(json.dumps(request))
                elif request['method'] == "lookup":
                    value = self.hashtable.lookup(request['key'])
                    if value:
                        status, value = {'status': 200, 'message': 'Success'}, {'requestData': {'request': 'lookup', 'key': request['key']}, 'result': {'value': value[1]}}
                    else:
                        status, value = {'status': 400, 'message': 'BadRequest'}, {'requestData': {'request': 'lookup', 'key': request['key'] }, 'result': {'error': 'no key in hashtable'}}
                elif request['method'] == "size":
                    value = self.hashtable.size()
                    status, value = {'status': 200, 'message': 'Success'}, {'requestData': {'request': 'size'}, 'result': {'value': value}}
                elif request['method'] == "query":
                    value = self.hashtable.query(request['subkey'], request['subvalue'])
                    status, value = {'status': 200, 'message': 'Success'}, {'requestData': {'request': 'query', 'subkey': request['subkey'], 'subvalue': request['subvalue']}, 'result': {'value': value}}
                    
        except KeyError:
            pass
        mesg = {'header': status, 'data': value}
        
        if conn:
            if status['status'] == 200:
                conn.sendall(f"{len(json.dumps(mesg).encode('utf-8')):>16}{json.dumps(mesg)}".encode('utf-8'))
            else:
                conn.sendall(f"{len(json.dumps(mesg).encode('utf-8')):>16}{json.dumps(mesg)}".encode('utf-8'))
        return 0
            
            
    def select_sockets(self):
        while True:
            try:
                while True:
                    try:
                        read_sockets, _, _ = select.select(self.socket_set, [], [])
                        break
                    except ValueError:
                        for sock in list(self.socket_set):
                            # Remove file descriptor if closed
                            if sock.fileno() < 0:
                                self.socket_set.remove(sock)
                                
                for socket in read_sockets:
                    if self.master == socket:
                        self.new_connection()
                    else:
                        self.recv_request(socket)
                
            except KeyboardInterrupt:
                print("\nServer killed by an interruption ( I hope you're happy (ㅠ﹏ㅠ) ).")
                sys.exit(1)

    
    def recv_request(self, curr_socket):
        curr_socket.settimeout(10)
        try:
            req = ""
            data = ""
            try:
                try:
                    size = curr_socket.recv(16)
                except ConnectionResetError:
                    self.socket_set.remove(curr_socket)
                    return
                
                try:
                    size = int(size.decode())
                except ValueError:
                    size = -1
                    pass
                while size > len(req):
                    get_bytes = size - len(req) if size-len(req) > 1024 else 1024
                    data = curr_socket.recv(get_bytes)
                    if not data:
                        self.socket_set.remove(curr_socket)
                        break
                    req += data.decode()
                    
                if not data:
                    curr_socket.close()
                    return
                
            except socket.timeout as e:
                self.socket_set.remove(curr_socket)
                curr_socket.close()
                return
                
            self.create_response(json.loads(req), curr_socket)
                    
        except KeyboardInterrupt:
            print("\nServer killed by an interruption ( I hope you're happy (ㅠ﹏ㅠ) ).")
            sys.exit(1)
        
        curr_socket.settimeout(None)
        return
    
    def new_connection(self):
        conn, _ = self.master.accept()
        self.socket_set.add(conn)
    
    
    def create_server(self):
        self.get_state()
        sock = socket.socket()
        self.master = sock
        self.socket_set.add(self.master)
        # print(self.hashtable.get_table()) # Checking to make sure table is correct
        
        sock.bind((self.host, self.port))
        # print(f"Listening on port {sock.getsockname()[1]}\nListening on host {socket.gethostname()}")
        self.port = int(sock.getsockname()[1])
        
        t = Thread(target=self.send_server_name, daemon=True)
        t.start()
        
        print(f"Listening on port {sock.getsockname()[1]}\nListening on host {socket.gethostname()}")
        sock.listen()
        self.port = sock.getsockname()[1]
        
        self.select_sockets()
        
        
    def checkpoint_flush(self):
        hashTableJson = json.dumps(self.hashtable.get_table(), indent=4)
        shadow = open('shadowtable.ckpt', 'w')
        
        shadow.write(hashTableJson)
        shadow.flush()
        os.fsync(shadow)
        
        try:
            os.rename('table.ckpt', 'delete.me')
            os.remove('delete.me')
        except FileNotFoundError:
            pass
            
        os.rename('shadowtable.ckpt', 'table.ckpt')
        os.remove('table.txn')
        self.transactionFile = None
        
    def transaction(self, trans: str):
        if not self.transactionFile:
            self.transactionFile = open('table.txn', 'ab')

        trans = f"{len(trans.encode('utf-8')):>16}{trans}"    
        self.transactionFile.write(trans.encode('utf-8'))
        self.transactionFile.flush()
        os.fsync(self.transactionFile)
        
        self.num_transactions += 1
        if self.num_transactions >= 100:
            self.checkpoint_flush()
            self.num_transactions = 0
    
    def get_state(self):
        try:
            checkpoint = open('table.ckpt', 'r')
            self.hashtable.set_table(json.load(checkpoint))
        except FileNotFoundError:
            try:
                os.rename('shadowtable.ckpt', 'table.ckpt')
                checkpoint = open('table.ckpt', 'r')
                self.hashtable.set_table(json.load(checkpoint))
                try:
                    os.remove('table.txn')
                except FileNotFoundError:
                    pass
                try:
                    os.remove('delete.me')
                except FileNotFoundError:
                    pass
            except FileNotFoundError:
                pass
            
        self.checkpoint = None
        
        try:
            self.transactionFile = open('table.txn', 'rb')
            self.read_transactions()
            self.transactionFile = None
        except FileNotFoundError:
            self.transactionFile = open('table.txn', 'ab')

    def read_transactions(self):
        while True:
            data = self.transactionFile.read(16)
            if not data:
                break
            
            try:
                data = int(data.decode('utf-8'))
            except ValueError:
                data = None 
            
            if not data:
                break
            
            byteString = self.transactionFile.read(data)
            if len(byteString) < data:
                self.checkpoint_flush()
                return
            
            self.num_transactions += 1
            self.create_response(json.loads(byteString.decode('utf-8')), False, True)

if __name__ == "__main__":
    
    try:
        int(sys.argv[1])
        server = HashTableServer(port=int(sys.argv[1]))
        server.create_server()
    except (IndexError, ValueError) as error:
        pass

    try:
        server = HashTableServer(server_name=sys.argv[1])
        
        server.create_server()
    except IndexError:
        print("Usage: python3 HashTableServer.py <Port Number | Server Name>")