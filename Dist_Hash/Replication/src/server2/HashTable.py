#!/usr/bin/env python3
'''This Library contains the basic functionalities onto a plain hash table'''

import re
import make_json

class HashTable:
    def __init__(self):
        self.dictionary = {}
        self.log_tally  = 0

    def insert(self, key, value):
        '''
            Return: formatted response ready to send to client 
        '''
        try:
            self.dictionary[key] = value 
            response = make_json.insert_response("OK", "Value Successfully Inserted")
        except TypeError as error:
            response = make_json.error_response("ERROR", "TypeError", f"Unhashable type: {type(key)}")
            
        return response
        
    def lookup(self, key):
        try:
            value = self.dictionary[key]
            response = make_json.lookup_response("OK", "Item Retrieved", value)
        except KeyError as error:
            response = make_json.error_response("ERROR", "KeyError", f"{key}") 
        return response

    def remove(self, key):
        try:
            value = self.dictionary.pop(key)
            response = make_json.remove_response("OK", "Item Deleted", value)
        except KeyError as error:
            response = make_json.error_response("ERROR", "KeyError", f"{key}") 
        return response

    def scan(self, regex):
        '''
            regex variable passed in as a string. 
            should be convereted into an r'' string

            Return: formatted response ready to send
        '''
        try:
            regex = re.compile(r'{}'.format(regex))
        except re.error as error:
            response = make_json.error_response("ERROR", "re.error", f"Invalid Regex: "+str(error))
            return response
        
        matches = []
        for key in self.dictionary:
            try:
                if regex.findall(key) != []:
                    matches.append(tuple([key, self.dictionary[key]]))
            except TypeError:
                continue
        response = make_json.scan_response("OK", "Scanned Items Returned", matches)

        return response 

def main():
    pass

if __name__ == "__main__":
    main()
