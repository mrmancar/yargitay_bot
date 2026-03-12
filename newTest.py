import ollama

texts = [
    "işçilik alacağına ilişkin karar",
    "kıdem tazminatı davası",
    "fazla mesai ücreti"
]

response = ollama.embed(
    model="qwen3-embedding",
    input=texts
)

print(response)
print(len(response.embeddings))
print(len(response.embeddings[0]))