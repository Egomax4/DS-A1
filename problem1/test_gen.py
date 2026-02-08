import random

def generate_test_file(filename, n_lines):
    # Sample vocabulary to create varied document frequencies
    subjects = ["apple", "banana", "cherry", "data", "elephant", "falcon"]
    verbs = ["eats", "process", "finds", "analyzes", "targets"]
    contexts = ["quickly", "silently", "in the cloud", "using python", "at scale"]

    with open(filename, "w") as f:
        for i in range(1, n_lines + 1):
            # Generate a random "sentence"
            sentence_parts = [
                random.choice(subjects),
                random.choice(verbs),
                random.choice(subjects),
                random.choice(contexts)
            ]
            # Ensure no newlines in text and join with spaces
            text = " ".join(sentence_parts)
            
            # Format: doc{i}\t{text}\n
            f.write(f"doc{i}\t{text}\n")

if __name__ == "__main__":
    N = 1000  # Number of lines
    generate_test_file("testcases.txt", N)
    print(f"Generated {N} lines in testcases.txt")