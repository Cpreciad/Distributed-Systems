#!/usr/bin/env python3
import json

def insert_request(key, value):
    '''
        creates the appropriate json request for an insert
        Returns: encoded json string to send to server
    '''
    #TODO check if value is already a json object
    # if it isn't, transform it into a json object 
    #TODO also, where do i check for a valid json
    request = json.dumps({"method": "insert", 
                "key":    key, 
                "value":  value})
    request_json = (str(len(request))+" " +request).encode('ascii')
    return request_json

def lookup_request(key):

    request = json.dumps({"method": "lookup", 
                "key":    key})
    request_json = (str(len(request))+" " +request).encode('ascii')
    return request_json

def remove_request(key):

    request = json.dumps({"method": "remove", 
                "key":    key})
    request_json = (str(len(request))+" " +request).encode('ascii')
    return request_json

def scan_request(regex):

    request = json.dumps({"method": "scan", 
                "regex":    regex})
    request_json = (str(len(request))+" " +request).encode('ascii')
    return request_json

def insert_response(status, message):

    response = json.dumps({"status": status, 
        "message": message,
        "method": "insert"})
                
    response_json = (str(len(response))+" " +response).encode('ascii')
    return response_json

def lookup_response(status, message, value):

    response = json.dumps({"status": status, 
        "message": message,
        "value": value,
        "method": "lookup"})
                
    response_json = (str(len(response))+" " +response).encode('ascii')
    return response_json

def remove_response(status, message, value):

    response = json.dumps({"status": status, 
        "message": message,
        "value": value,
        "method":"remove"})
                
    response_json = (str(len(response))+" " +response).encode('ascii')
    return response_json

def scan_response(status, message, matches):

    response = json.dumps({"status": status, 
        "message": message,
        "matches": matches,
        "method":"scan"})
                
    response_json = (str(len(response))+" " +response).encode('ascii')
    return response_json

def error_response(status, error, message):
    response = json.dumps({"status": status, "error": error, "message": message})
    response_json = (str(len(response))+ " " + response).encode('ascii')
    return response_json

def main():
    ret_val = insert("hello", "world")
    print(type(ret_val))
    

if __name__ == "__main__":
    main()
