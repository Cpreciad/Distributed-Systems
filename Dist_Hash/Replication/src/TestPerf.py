#!/usr/bin/env python3
import sys
import time
import os
import hashlib
import HashTableClient
import http.client
import pprint
import json
import ClusterClient

HOST = "catalog.cse.nd.edu"
PORT = 9097

def usage(status):
    print("Usage: ./TestPerf.py [hostname] [port]")
    sys.exit(status)

def time_insert(table):
    '''Measure latency and bandwith of insert operation'''

    count = 0
    start_time = time.time_ns()
    while time.time_ns() - start_time <= 3000000000:
        try:
            table.insert(f'{hashlib.md5(str(count).encode("ascii")).hexdigest()}',f'{count}')
        except:
            print('error insert')
            pass
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

    while time.time_ns() - start_time <= 3000000000:
        try:
            table.lookup(f'{hashlib.md5(str(count).encode("ascii")).hexdigest()}')
        except:
            pass
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
    while time.time_ns() - start_time <= 3000000000:
        try:
            table.scan(f'{hashlib.md5(str(count).encode("ascii")).hexdigest()}')
        except:
            pass
        count += 1


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
    while time.time_ns() - start_time <= 3000000000:
        try:
            table.remove(f'{hashlib.md5(str(count).encode("ascii")).hexdigest()}')
        except:
            pass
        count += 1


    end_time = time.time_ns()
    total_time = end_time - start_time
    
    print("Remove Operation:")
    print(f"Total Time:       {(total_time) / (1000000000)} seconds")
    print(f"Total Operations: {count}")
    print(f"Throughput:       {count / (total_time / (1000000000)):.0f} operations/second")
    print(f"Latency :         {total_time / count:.0f} nanoseconds/operation\n")

def main():
    '''driver function to test performance of each operation'''
    if len(sys.argv) != 4:
        usage(1)
    hostname = sys.argv[1]
    N        = int(sys.argv[2])
    K        = int(sys.argv[3])
    table =  ClusterClient.ClusterClient(hostname, N, K)
    time_insert(table)
    time_lookup(table)
    time_scan(table)
    time_remove(table)

if __name__ == '__main__':
    main()
