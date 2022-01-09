#!/usr/bin/env python3
import sys
import socket
import json
import make_json
import HashTable
import os
import select
import time

# constants
HOST        = ''
PORT        = 0
SOCK_MAXCON = 1
BUFSIZ      = 4096
MAXLOGS     = 100

UDP_HOST    = "catalog.cse.nd.edu"
UDP_PORT    = 9097

def usage(status):
    print('Usage: ./HashTableServer.py [custom server name]')
    sys.exit(status)

def dump_checkpoint(table):
    '''
        This function is responsible for an automic dump of
        a checkpoint
    '''
    store_table = json.dumps(table.dictionary)
    with open('temp.ckpt', 'w') as ckpt:
        ckpt.write(store_table)
        ckpt.flush()
        os.fsync(ckpt)
    os.rename('temp.ckpt', 'table.ckpt')

def update_log(table, request):
    '''
        this function updates the log file, and compats a new 
        checkpoint file as necessagy
    '''
    table.log_tally += 1

    request_text = json.dumps(request)

    if table.log_tally >= MAXLOGS:
        dump_checkpoint(table)
        os.remove('table.txn')
        table.log_tally = 0

    else:
        with open('table.txn', 'a+') as txn:
            txn.write(f"{request_text}\n")
            txn.flush()
            os.fsync(txn)

def load_checkpoint(table):
    '''
	    This function loads the checkpoint file if they exist, 
	    if they do not exist, then this file creates table.ckpt and table.txn
    '''
    try:
        with open('table.ckpt', 'r+') as ckpt:
            table.dictionary = json.load(ckpt)
        print('loaded checkpoint file')

    except:
        pass
    try:
        with open('table.txn' ,'r+') as txn:
            for request in txn.readlines():
                table.log_tally += 1
                request = json.loads(request.strip())
                if request.get('method') == 'insert':
                    table.insert(request['key'], request['value'])
                if request.get('method') == 'remove':
                    table.remove(request['key'])
                else:
                    pass
        print(f'transaction logs loaded: {table.log_tally}')
    except:
        with open('temp.txn', 'w') as txn:
            txn.flush()
            os.fsync(txn)
        os.rename('temp.txn', 'table.txn')

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
    

def handle_request(client_socket, table): 
    '''
        Simple request handling funciton which deals with
        fragmented data. dictionary methods will be invoked based on
        the client request
        returns False when the client socket disconnects
    '''
        # first time called to recieve the clients request
    try:
        data = client_socket.recv(BUFSIZ)
    except:
        # catches a timeout
        client_socket.close()
        return False
    if not data:
        #break
        return False
    # initially segment data into sent bytes and request message
    try:
        sent_bytes, request = data.decode('ascii').split(" ", 1)
        recieved_bytes = len(request)
        sent_bytes = int(sent_bytes)

    # if message is not formatted correctly, cut off connection 
    except ValueError as error:
        return False
    # read in remaining bytes from client message
    while int(sent_bytes) > recieved_bytes:
        try:
            frag_request = (client_socket.recv(BUFSIZ)).decode('ascii')
        except:
            client_socket.close()
            return False
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
            return False
        response = process_request(request, table)
    else:
        client_socket.close()
        return False

    # respond to client. server needs only sendall once 
    client_socket.sendall(response) 
    return True



def send_udp(sock, custom_hostname, port):
   
    udp_message = {"type"   : "hashtable",
                   "owner"  : "cpreciad",
                   "port"   : port,
                   "project": custom_hostname
                  }
    udp_message = json.dumps(udp_message)
    udp_message = udp_message.encode('ascii')
    sock.sendto(udp_message, (UDP_HOST, UDP_PORT))

def main():
    # Read flags for port number
    try:
        custom_hostname  = sys.argv[1]
    except:
        usage(1)
    # create server listening socket with a given port
    sock = socket_listen(PORT)
    port = sock.getsockname()[1]
    print(sock.getsockname())
    print(f'listening on port {port}')

    # set up udp socket for name recognition
    #TODO find a way to not pollute the catalog server
    udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    start_time = time.time()
    send_udp(udp_sock, custom_hostname, port)

    # accept client, handle request
    table = HashTable.HashTable()
    # load in checkpoint and transaction log if they exist
    load_checkpoint(table)
    # automatically add the server socket to the list, instantiate the list
    socket_list = [sock]
    # define the client data dictionary
    client_data = {}
    while True:
        if time.time() - start_time > 60:
            send_udp(udp_sock, custom_hostname, port)
            start_time = time.time()
        rlist, _, _ = select.select(socket_list, [],[], 2.0)
        for s in rlist:
            if sock == s:
                client_info = accept_client(s)
                print(f'new client connected: {client_info[0].getsockname()[0]}')
                socket_list.append(client_info[0])
                client_data[client_info[0].getsockname()[0]] = client_info[0]

            else:
                client_key = s.getsockname()[0]
                status = handle_request(s, table)
                if status == False:
                    socket_list.remove(s)
                    try:
                        client_data.pop(client_key)
                    except KeyError:
                        pass
                    s.close()
                    print(f'client disconnected: {client_key}')


    
if __name__ == "__main__":
    main()
