#!/usr/bin/env python3
"""
Simple test script for parallel data collection and sending
Simulates collecting data from multiple terminals and sending to a destination
"""

import time
import random
from multiprocessing import Process, current_process
from datetime import datetime


def collect_from_terminal(terminal_id):
    """Simulate collecting data from a terminal"""
    print(f"[{current_process().name}] Collecting data from Terminal {terminal_id}...")
    
    # Simulate data collection delay (like reading from terminal)
    time.sleep(random.uniform(0.5, 2.0))
    
    # Simulate collected data
    data = {
        'terminal_id': terminal_id,
        'timestamp': datetime.now().isoformat(),
        'value': random.randint(100, 999),
        'status': 'ok'
    }
    
    print(f"[{current_process().name}] Collected from Terminal {terminal_id}: {data}")
    return data


def send_to_destination(data, destination):
    """Simulate sending data to a destination"""
    print(f"[{current_process().name}] Sending data to {destination}...")
    
    # Simulate network delay
    time.sleep(random.uniform(0.3, 1.0))
    
    print(f"[{current_process().name}] Successfully sent data from Terminal {data['terminal_id']} to {destination}")
    return True


def collect_and_send(terminal_id, destination):
    """Main worker function: collect data and send it"""
    process_name = current_process().name
    print(f"\n[{process_name}] Starting work for Terminal {terminal_id}")
    
    try:
        # Step 1: Collect data
        data = collect_from_terminal(terminal_id)
        
        # Step 2: Send data
        send_to_destination(data, destination)
        
        print(f"[{process_name}] Completed work for Terminal {terminal_id}\n")
        
    except Exception as e:
        print(f"[{process_name}] ERROR for Terminal {terminal_id}: {e}")


def main():
    """Main function to run parallel data collection"""
    print("=" * 60)
    print("Starting Parallel Data Collection Test")
    print("=" * 60)
    
    # Configuration
    terminals = [1, 2, 3, 4, 5]  # List of terminal IDs
    destination = "192.168.1.100:8080"  # Destination address
    
    # Create a process for each terminal
    processes = []
    
    start_time = time.time()
    
    for terminal_id in terminals:
        p = Process(
            target=collect_and_send,
            args=(terminal_id, destination),
            name=f"Worker-{terminal_id}"
        )
        processes.append(p)
        p.start()
        print(f"Started process for Terminal {terminal_id}")
    
    # Wait for all processes to complete
    for p in processes:
        p.join()
    
    end_time = time.time()
    
    print("=" * 60)
    print(f"All processes completed in {end_time - start_time:.2f} seconds")
    print("=" * 60)


if __name__ == "__main__":
    main()