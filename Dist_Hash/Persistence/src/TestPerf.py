#!/usr/bin/env python3
import sys
import time
import os
import hashlib
import HashTableClient

def usage(status):
    print("Usage: ./TestPerf.py [hostname] [port]")
    sys.exit(status)
    
def big_data():

    with open('input.txt', 'w') as in_file:
        for i in range(0, 5000):
            in_file.write(f'{hashlib.md5(str(i).encode("ascii")).hexdigest()} {i}\n')


def time_insert(table):
    '''Measure latency and bandwith of insert operation'''

    count = 0
    start_time = time.time_ns()

    # insert 1000 random strings into dictionary
    for line in open('input.txt', 'r'):
        key, value = line.split()
        response = table.insert(key, value)
        count += 1

    end_time = time.time_ns()
    total_time = end_time - start_time
    
    print("Insert Operation:")
    print(f"Total Time:       {(total_time) / (1000000000)} seconds")
    print(f"Total Operations: {count}")
    print(f"Throughput:       {count / (total_time / (1000000000)):.0f} operations/second")
    print(f"Latency :         {total_time / count:.0f} nanoseconds/operation\n")


def time_lookup(table):
    '''Measure latency and bandwith of lookup operation'''

    count = 0
    start_time = time.time_ns()

    # insert 1000 random strings into dictionary
    for line in open('input.txt', 'r'):
        key, value = line.split()
        response = table.lookup(key)
        count += 1

    end_time = time.time_ns()
    total_time = end_time - start_time
    
    print("Lookup Operation:")
    print(f"Total Time:       {(total_time) / (1000000000)} seconds")
    print(f"Total Operations: {count}")
    print(f"Throughput:       {count / (total_time / (1000000000)):.0f} operations/second")
    print(f"Latency :         {total_time / count:.0f} nanoseconds/operation\n")



def time_scan(table):
    '''Measure latency and bandwith of scan operation'''

    count = 0
    start_time = time.time_ns()

    # insert 1000 random strings into dictionary
    for line in open('input.txt', 'r'):
        regex, value = line.split()
        response = table.scan(regex)
        count += 1
        if count == 100:
            break

    end_time = time.time_ns()
    total_time = end_time - start_time
    
    print("Scan Operation:")
    print(f"Total Time:       {(total_time) / (1000000000)} seconds")
    print(f"Total Operations: {count}")
    print(f"Throughput:       {count / (total_time / (1000000000)):.0f} operations/second")
    print(f"Latency :         {total_time / count:.0f} nanoseconds/operation\n")


def time_remove(table):
    '''Measure latency and bandwith of remove operation'''

    count = 0
    start_time = time.time_ns()

    for line in open('input.txt', 'r'):
        regex, value = line.split()
        response = table.remove(regex)
        count += 1

    end_time = time.time_ns()
    total_time = end_time - start_time
    
    print("Remove Operation:")
    print(f"Total Time:       {(total_time) / (1000000000)} seconds")
    print(f"Total Operations: {count}")
    print(f"Throughput:       {count / (total_time / (1000000000)):.0f} operations/second")
    print(f"Latency :         {total_time / count:.0f} nanoseconds/operation\n")


def main():
    '''Runner function to test performance of each operation'''
    if len(sys.argv) != 3:
        usage(1)
    host = sys.argv[1]
    port = int(sys.argv[2])
    table = HashTableClient.HashTable(host,port)

    big_data()
    time_insert(table)
    #time_lookup(table)
    #time_scan(table)
    #time_remove(table)
    os.remove("input.txt")

if __name__ == '__main__':
    main()
