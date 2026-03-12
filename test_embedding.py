import ollama

response = ollama.embeddings(
    model="qwen3-embedding",
    prompt="merhaba dünya"
)

print(len(response["embedding"]))