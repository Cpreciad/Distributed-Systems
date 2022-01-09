#!/usr/bin/env python3
import sys
import socket
import os
import json
import make_json
import re
import http.client
# constants
BUFSIZ = 4097
# name server
HOST = "catalog.cse.nd.edu"
PORT = 9097


def find_server(hostname):
    ''' 
        makes an http get request for the hashtableserver name 
        returns the host and port
    '''
    client = http.client.HTTPConnection(HOST, PORT) 
    client.request("GET","/query.json")
    response = client.getresponse().read()
    response = json.loads(response)
    for entry in response:
        if entry["type"] == "hashtable" and entry["project"] == hostname:
            socket = socket_connect(entry["address"], entry["port"])
            if socket == None:
                continue
            return socket

    return None

# functions for the class
def socket_connect(hostname, port):
    ''' 
    Create and connect to a server
    Return:
        Successful  : Client Socket object
        Unsuccessful: None Type
    ''' 

    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    except:
        print('Could not Create a socket')
        return None
 
    try:
        s.connect((hostname, port))
    except: 
        print('Could not connect to Service')
        return None

    return s

def usage(status):
    pass

def decode_response(response):
    '''
        Returns the appropriate response to 
        the client that called the request
    '''
    
    response = json.loads(response)
    if response.get('error'):
        if response['error'] == "TypeError":
            raise TypeError(f'{response["message"]}')
        elif response['error'] == "KeyError":
            raise KeyError(f'{response["message"]}')
        elif response['error'] == "re.error":
            raise re.error(f'{response["message"]}')
        else:
            raise Exception(f"Unidentified problem")


    elif   response['method'] == "insert":
        return f"{response['status']}: {response['message']}"

    elif response['method'] == "lookup":
        return_response = response['value']
        return return_response

    elif response['method'] == "remove":
        return_response = response['value']
        return return_response
        
    elif response['method'] == "scan":
        return_response = response['matches']
        return return_response
    else:
       raise Exception(f"this shouldn't have happened, response was invalid")

def send_request(socket, request):
        status = socket.sendall(request)
        #TODO implement timeouts for resending and giving up 
        data = socket.recv(BUFSIZ)

        sent_bytes, response = data.decode('ascii').split(" ", 1)
        recieved_bytes = len(response)
        
        # read remaining messages
        while int(sent_bytes) > recieved_bytes:
            response += (socket.recv(BUFSIZ).decode('ascii'))
            recieved_bytes = len(response)
        
        response = decode_response(response)

        return response

class HashTable:

    def __init__(self, hostname):
        try:
            self.socket = find_server(hostname) 
        except:
            self.socket = None

    def insert(self, key, value):
    
        request = make_json.insert_request(key, value)
        response = send_request(self.socket, request)
        return response

    def lookup(self, key):

        request = make_json.lookup_request(key)
        response = send_request(self.socket, request)
        return response

    def remove(self, key):
    
        request = make_json.remove_request(key)
        response = send_request(self.socket, request)
        return response

    def scan(self, regex):

        request = make_json.scan_request(regex)
        response = send_request(self.socket, request)
        return response

    def disconnect(self):
        self.socket.close()

