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

# import random
# import os

# def generate_stress_test(filename, n_docs, target_vocab_size=150):
#     # 1. Create a specific vocabulary size (50-200)
#     # Using hex-like strings to ensure they are unique and uniform
#     vocab = [f"word_{i:03d}" for i in range(target_vocab_size)]
    
#     # To hit ~100MB with 100k docs, each line needs to be roughly 1000 bytes
#     # 100,000,000 bytes / 100,000 docs = 1,000 chars per doc
#     words_per_doc = 120 

#     print(f"Generating {n_docs} documents...")
    
#     with open(filename, "w") as f:
#         for i in range(1, n_docs + 1):
#             # Pick random words from our controlled vocab
#             content = " ".join(random.choices(vocab, k=words_per_doc))
            
#             # Format: docID\tContent
#             f.write(f"doc{i}\t{content}\n")
            
#             if i % 20000 == 0:
#                 print(f"Progress: {i} documents written...")

#     file_size = os.path.getsize(filename) / (1024 * 1024)
#     print(f"\nSuccess! File: {filename}")
#     print(f"Final Size: {file_size:.2f} MB")
#     print(f"Vocabulary Size: {len(vocab)} words")

# if __name__ == "__main__":
#     generate_stress_test("stress_test.txt", 100000)