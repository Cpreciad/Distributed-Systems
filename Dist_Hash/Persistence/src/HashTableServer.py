#!/usr/bin/env python3
import sys
import socket
import json
import make_json
import HashTable
import os

# constants
HOST        = ''
SOCK_MAXCON = 1
BUFSIZ      = 4096
MAXLOGS     = 99

def usage(status):
    print('Usage: ./HashTableServer.py [port number]')
    sys.exit(status)

def dump_checkpoint(table):
    store_table = json.dumps(table.dictionary)
    with open('temp.ckpt', 'w') as ckpt:
        ckpt.write(store_table)
        ckpt.flush()
        os.fsync(ckpt)
    os.rename('temp.ckpt', 'table.ckpt')

def update_log(table, request):
    table.log_tally += 1

    request_text = json.dumps(request)

    if table.log_tally == MAXLOGS:
        dump_checkpoint(table)
        with open('table.txn', 'w') as txn:
            # clears the transaction log file
            pass
        table.log_tally = 0

    else:
        with open('table.txn', 'a+') as txn:
            txn.write(request_text)
            txn.write("\n")

def load_checkpoint(table):
    try:
        with open('table.ckpt', 'r+') as ckpt:
            table.dictionary = json.load(ckpt)

    except:
        with open('table.ckpt', 'w') as ckpt:
            pass
    try:
        with open('table.txn' ,'r+') as txn:
            for request in txn.readlines():
                request = json.loads(request.strip())
                process_request(request, table)
    except:
        with open('table.txn', 'w') as txn:
            pass
def socket_listen(port):
    ''' 
        Creates and returns a listening socket, Goes through checks for 
            valid socket binding
        Return: Successful:   socket object
                Unsuccessful: None Type
    '''    
    # create socket "fd" (more like socket object)
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    except Exception as error:
        print(repr(error))
        return None
    # bind the socket
    try:
        s.bind((HOST, port))
    except Exception as error:
        print(repr(error))
        s.close()
        return None
    # transform socket into a listening socket
    try:
        s.listen(SOCK_MAXCON)
    except Exception as error:
        print(repr(error))
        s.close()
        return None

    return s

def accept_client(s):
    ''' 
        Accepts a single client, returns a client socket for processing
            and ensures a client socket was created
        Return: Successful:   (socket object, client address)
                Unsuccessful: None Type
    '''
    try:
        client_socket, client_address  = s.accept()
    except Exception as error:
        print(repr(error))
        return None
    return (client_socket, client_address)
def process_request(request, table):
    '''
        This function parses the request dict and checks:
        length of the operations is correct, 
        operates on the given method (insert, remove, lookup, scan),
        and handles all errors in the process

        Return: a tuple containing the status and response
    '''
    if request.get("method") == "insert":
        update_log(table, request)
        response = table.insert(request['key'], request['value'])

    elif request.get("method") == "lookup":
        response = table.lookup(request['key'])

    elif request.get("method") == "remove":
        update_log(table, request)
        response = table.remove(request['key'])

    elif request.get("method") == "scan":
        response = table.scan(request['regex'])
    
    else:
        response = make_json.error_response("ERROR", "Unknown Operation")

    return response
    

def handle_request(client_socket, client_address, table): 
    '''
        Simple request handling funciton which deals with
        fragmented data. dictionary methods will be invoked based on
        the client request
    '''
    while True:
        # first time called to recieve the clients request
        try:
            data = client_socket.recv(BUFSIZ)
        except:
            # catches a timeout
            client_socket.close()
            print('client connection closed')
            return
        if not data:
            break
        # initially segment data into sent bytes and request message
        try:
            sent_bytes, request = data.decode('ascii').split(" ", 1)
            recieved_bytes = len(request)
            sent_bytes = int(sent_bytes)

        # if message is not formatted correctly, cut off connection 
        except ValueError as error:
            client_socket.close()
            print('client connection closed')
            return
        # read in remaining bytes from client message
        while int(sent_bytes) > recieved_bytes:
            try:
                frag_request = (client_socket.recv(BUFSIZ)).decode('ascii')
            except:
                client_socket.close()
                print('client connection closed')
                return
            if not frag_request:
                break
            request += frag_request
            recieved_bytes = len(request)
        
        # after reading in all data, process request if the message is complete
        if sent_bytes == recieved_bytes:
            try:
                # this will only process a complete json message
                request = json.loads(request)
            except json.decoder.JSONDecodeError as error:
                client_socket.close()
                print('client connection closed')
                return 
            response = process_request(request, table)
        else:
            client_socket.close()
            print('client connection closed')
            return

        # respond to client. server needs only sendall once 
        client_socket.sendall(response) 
    print('client connection closed')
    client_socket.close()
 
def main():
    # Read flags for port number
    try:
        port  = int(sys.argv[1])
    except:
        usage(1)
    # create server listening socket with a given port
    sock = socket_listen(port)
    print(f'listening on port {sock.getsockname()[1]}')

    # accept client, handle request
    table = HashTable.HashTable()
    load_checkpoint(table)
    while True:
        #TODO find a way to deal with a failed connection
        client_info = accept_client(sock)
        print(f'new client connected: {client_info[0].getsockname()[0]}')
        handle_request(*client_info, table)

    
if __name__ == "__main__":
    main()
