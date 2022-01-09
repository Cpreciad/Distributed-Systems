#!/usr/bin/env python3
import HashTableClient
import hashlib
import time

def my_hash(key):
        
    return int(hashlib.md5(key.encode('ascii')).hexdigest(), 16)

def connect_clients(base_name, N):
    '''
    create N clients for N servers, and return them all in a list
    '''
    clients = []
    
    for i in range(N):
        table = HashTableClient.HashTable(f'{base_name}-{i}')
        clients.append(table)
    return clients

class ClusterClient:

    def __init__(self, base_name, N, K):
        # N is the number of clients for matching the number of servers
        self.num_servers = N
        self.rep_factor  = K 
        self.clients = connect_clients(base_name, N)
    
         
    def insert(self, key, value):
         
        hashed_key = my_hash(key) % self.num_servers 
        
        result = 'AttributeError'
        for k in range(self.rep_factor):
            while result == 'AttributeError':
                try:
                    result = self.clients[(hashed_key + k) % self.num_servers].insert(key, value)
                except AttributeError:
                    result = 'AttributeError'
                    time.sleep(5) 
        return result

    def lookup(self, key):
        
        hashed_key = my_hash(key) % self.num_servers 
        
        result = 'AttributeError'
        key_errors = 0
        while result == 'AttributeError':
            for k in range(self.rep_factor):
                try:
                    result = self.clients[(hashed_key + k) % self.num_servers].lookup(key)
                    break
                except KeyError:
                    key_errors += 1
                except AttributeError:
                    result = 'AttributeError'
                    time.sleep(5)
                if key_errors >= self.rep_factor:
                    raise KeyError('Replicatoin Factor (K) too small. Could not find Data')

        return result 
       
    def remove(self, key):

        hashed_key = my_hash(key) % self.num_servers 

        result = 'AttributeError'
        for k in range(self.rep_factor):
            while result == 'AttributeError':
                try:
                    result = self.clients[(hashed_key + k) % self.num_servers].remove(key)
                except AttributeError:
                    result = 'AttributeError'
                    time.sleep(5)
                    continue

        return result 

    def scan(self, regex):
        total_results = []
        
        result = 'AttributeError'
        for client in self.clients:
            while result == 'AttributeError':
                try:
                    total_results.extend(client.scan(regex))
                    result = 'Good'
                except AttributeError:
                    result = 'AttributeError'
                    time.sleep(5)


        return total_results


    def disconnect(self):
        for client in self.clients:
            client.disconnect()

def main():
    cluster = ClusterClient("test", 2, 1)
    print(cluster.insert("key", "hello"))
    print(cluster.insert("new_key", "new_hello"))
    print(cluster.insert("ew_key", "new_hello"))
    


if __name__ == "__main__":
    main()








