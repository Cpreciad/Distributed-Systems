#!/usr/bin/env python3
import sys
import time
import os
import hashlib
import HashTableClient
def time_insert(table):
    '''Measure latency and bandwith of insert operation'''
    fastest = sys.maxsize
    slowest = 0
    count = 0 
    start_time = time.time_ns()
    while time.time_ns() - start_time <= 3000000000:
        interm_start_time = time.time_ns()
        table.insert(f'{hashlib.md5(str(count).encode("ascii")).hexdigest()}',f'{count}')
        interm_end_time = time.time_ns()
        if interm_end_time - interm_start_time <= fastest:
            fastest = interm_end_time - interm_start_time
        if interm_end_time - interm_start_time >= slowest:
            slowest = interm_end_time - interm_start_time
        count += 1
    
    end_time = time.time_ns()
    total_time = end_time - start_time
    
    print("Insert Operation:")
    print(f"Total Time:       {(total_time) / (1000000000):.4f} seconds")
    print(f"Averave Time:     {(total_time) / (1000000000)/count:.4f} seconds")
    print(f"Fastest Time:     {(fastest) / (1000000000):.4f} seconds")
    print(f"SlowestTime:      {(slowest) / (1000000000):.4f} seconds")



def time_pairs(table):
    '''Measure latency and bandwith of pairs operation'''
    fastest_insert = sys.maxsize
    slowest_insert = 0
    fastest_remove = sys.maxsize
    slowest_remove = 0
    count = 0 
    count = 0
    start_time = time.time_ns()
    while time.time_ns() - start_time <= 5000000000:
        interm_start_time = time.time_ns()
        table.insert(f'{hashlib.md5(str(count).encode("ascii")).hexdigest()}',f'{count}')
        interm_end_time = time.time_ns()
        if interm_end_time - interm_start_time <= fastest_insert:
            fastest_insert = interm_end_time - interm_start_time
        if interm_end_time - interm_start_time >= slowest_insert:
            slowest_insert = interm_end_time - interm_start_time
        
        interm_start_time = time.time_ns()
        table.remove(f'{hashlib.md5(str(count).encode("ascii")).hexdigest()}')

        interm_end_time = time.time_ns()
        if interm_end_time - interm_start_time <= fastest_remove:
            fastest_remove = interm_end_time - interm_start_time
        if interm_end_time - interm_start_time >= slowest_remove:
            slowest_remove = interm_end_time - interm_start_time
        count += 2  


    end_time = time.time_ns()
    total_time = end_time - start_time

    print("Pairs Operation:")
    print(f"Total Time:         {(total_time) / (1000000000):.4f} seconds")
    print(f"Total Operations:   {count}")
    print(f"Averave Time:       {(total_time) / (1000000000)/count:.4f} seconds")
    print(f"Fastest Insert:     {(fastest_insert) / (1000000000):.4f} seconds")
    print(f"Slowest Insert:     {(slowest_insert) / (1000000000):.4f} seconds")
    print(f"Fastest Remove:     {(fastest_remove) / (1000000000):.4f} seconds")
    print(f"Slowest Remove:     {(slowest_remove) / (1000000000):.4f} seconds")

def usage():
    print("Usage: ./TestOutliars.py [hostname]")
    sys.exit(0)

def main():
    '''driver function to test performance of each operation'''
    if len(sys.argv) != 2:
        usage()
    host = sys.argv[1]
    table = HashTableClient.HashTable(host)

    time_pairs(table)

if __name__ == '__main__':
    main()

