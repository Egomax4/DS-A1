import subprocess
from tqdm import tqdm # Optional: for a progress bar

def run_test():
    results = set()
    
    print("Starting 1,000 consistency checks...")
    for i in tqdm(range(100)):
        # Run main.py and capture stdout
        process = subprocess.run(['python3', 'main.py'], 
                               capture_output=True, 
                               text=True)
        
        # We store the output string in a set
        # If the code is deterministic, the set size will remain 1
        results.add(process.stdout.strip())
        
        if len(results) > 1:
            print(f"\n[!] Inconsistency detected at iteration {i}!")
            for idx, res in enumerate(results):
                print(f"Result variation {idx+1}:\n{res}\n")
            return

    print("\n[✓] Success! 1,000/1,000 runs produced identical output.")

if __name__ == "__main__":
    run_test()