#!/usr/bin/env python3
import sys
import argparse
from kvstore import KVStore


def run_interactive(store: KVStore):
    """Run the interactive command-line interface."""
    print("Key-Value Store CLI")
    print("Commands: SET <key> <value>, GET <key>, EXIT")
    print("-" * 50)
    
    while True:
        try:
            user_input = input("> ").strip()
            
            if not user_input:
                continue
            
            parts = user_input.split(None, 2)
            command = parts[0].upper()
            
            if command == "EXIT" or command == "QUIT":
                print("Goodbye!")
                break
            
            elif command == "SET":
                if len(parts) < 3:
                    print("Error: SET requires <key> <value>")
                    continue
                key = parts[1]
                value = parts[2]
                store.set(key, value)
                print(f"OK")
            
            elif command == "GET":
                if len(parts) < 2:
                    print("Error: GET requires <key>")
                    continue
                key = parts[1]
                value = store.get(key)
                if value is not None:
                    print(value)
                else:
                    print("(nil)")
            
            elif command == "KEYS":
                keys = store.keys()
                if keys:
                    for key in keys:
                        print(f"  {key}")
                else:
                    print("(empty)")
            
            elif command == "DELETE" or command == "DEL":
                if len(parts) < 2:
                    print("Error: DELETE requires <key>")
                    continue
                key = parts[1]
                if store.delete(key):
                    print("OK")
                else:
                    print("Error: Key not found")
            
            elif command == "HELP":
                print("Available commands:")
                print("  SET <key> <value> - Set a key-value pair")
                print("  GET <key>         - Get the value for a key")
                print("  DELETE <key>      - Delete a key")
                print("  KEYS              - List all keys")
                print("  HELP              - Show this help message")
                print("  EXIT              - Exit the program")
            
            else:
                print(f"Error: Unknown command '{command}'. Type HELP for available commands.")
        
        except KeyboardInterrupt:
            print("\nGoodbye!")
            break
        except Exception as e:
            print(f"Error: {e}")


def main():
    parser = argparse.ArgumentParser(description="Command-line key-value store with append-only storage")
    parser.add_argument(
        "--storage",
        default="kvstore.log",
        help="Path to the storage file (default: kvstore.log)"
    )
    
    args = parser.parse_args()
    
    store = KVStore(args.storage)
    run_interactive(store)


if __name__ == "__main__":
    main()
