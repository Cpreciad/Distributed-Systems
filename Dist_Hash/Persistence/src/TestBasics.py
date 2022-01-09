#!/usr/bin/env python3
import HashTableClient as Hash
import sys
import hashlib
'''This Program is used to test the operations of the RPC hash table'''

def main():
    # create client connection to server    
    host = str(sys.argv[1])
    port = int(sys.argv[2])
    table = Hash.HashTable(host,port)
    # simple insert, lookup, remove
    print("Testing Conventional Operations ")
    response = table.insert("key", {"a_key":"value"})
    print(response)

    value = table.lookup("key")
    print(f"lookup value: {value}")

    table.insert("another_key", {"an_key":"an_value"})
    value = table.remove("another_key")
    print(f"removed value from 'another_key': {value}")

    table.insert("Carlo", {"Name": "Carlo Preciado", "Age": "21",
        "Hobbies": ["Soccer", "Basketball", "Surfing"]})
    table.insert("Carlito", {"Name": "Carlo Preciado", "Age": "21",
        "Hobbies": ["Soccer", "Basketball", "Surfing"]})

    # insert unconventional keys and values
    print("")
    print("Testing unconventional operations")
    response = table.insert(1, {2:3})
    print(f"Status of inserting integer as key: {response}")
    print(f'lookup value: {table.lookup(1)}')
    response = table.insert(None, None)
    print(f"Status of inserting None Type as key: {response}")
    print(f'lookup value: {table.lookup(None)}')
    print("")
    print("Testing error inducing operations")
    print("inserting with an unhashable key")
    try:
        response = table.insert([1,2], {"invalid": "unhashable"})
    except Exception as error:
        print(str(error))

    print("looking up a key that doesn't exist")
    try:
        response = table.lookup("another_key")
    except Exception as error:
        print(str(error)) 
    print("removing a key that doesn't exist")
    try:
        response = table.remove("another_key")
    except Exception as error:
        print(str(error)) 
    

    table.disconnect()
    
    print("")
    print("new client connected, showing data persistance by scanning data")
    print("")
    print("scanning with .*")
    table2 = Hash.HashTable(host,port)
    matches = table2.scan(".*")
    for match in matches:
        print(match)

    print("")
    print("scanning with C.*")
    matches = table2.scan("C.*")
    for match in matches:
        print(match)
    
    print("")
    print("scanning with an invalid regex")
    try:
        matches = table2.scan("[")
    except Exception as error:
        print(str(error))
    
    print("")
    print("Clean up hash table by scanning all keys with .* and removing all keys")
    
    matches = table2.scan(".*")
    for match in matches:
        table2.remove(match[0])
    
    print("resulting data after cleanup:")
    print(table2.scan(".*"))


if __name__ == "__main__":
    main()
