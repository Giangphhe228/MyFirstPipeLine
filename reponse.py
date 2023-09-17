from elasticsearch import Elasticsearch
from google.cloud import storage
from vertexai.language_models import TextEmbeddingModel, TextGenerationModel

model = TextEmbeddingModel.from_pretrained("textembedding-gecko@001")

# testcase1
# QUESTION = "tóm tắt các bước xử lý ram cao"


QUESTION = "tóm tắt các bước cảnh báo ổ đĩa dung lượng"

# Connect to Elasticsearch
es = Elasticsearch(
    hosts=["http://localhost:9200"],
    basic_auth=("elastic", "Yv6qjBp8by754cujC7348M5h")
)

# Index name
index_name = 'work_flow_instruction_documents'

# Search for the nearest neighbors
embeddings = model.get_embeddings([QUESTION])
script_query = {
    "script_score": {
        "query": {"match_all": {}},
        "script": {
            "source": "cosineSimilarity(params.query_vector, 'embedding')",
            "params": {"query_vector": embeddings[0].values}
        }
    }
}

# Execute the search
res = es.search(index=index_name, query=script_query)

def download_blob(bucket_name, source_blob_name):
    """Downloads a blob from the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    return blob.download_as_text()

bucket_name = "chatbot-leanring-data-pdf"
source_blob_name = res["hits"]["hits"][0]['_id'].replace("gs://", "").replace(bucket_name + "/", "")
print("source_blob_name",)
# Download the text content of the file
text_content = download_blob(bucket_name, source_blob_name)

parameters = {
    "temperature": 0.2,
    "max_output_tokens": 256,
    "top_p": 0.8,
    "top_k": 40
}
prompt = f"""
Nếu tài liệu bên dưới hữu ích hãy sử dụng nó để trả lời câu hỏi:

{text_content}

{QUESTION}
"""
model = TextGenerationModel.from_pretrained("text-bison@001")
response = model.predict(prompt, **parameters)

print(f"Prompt: {prompt}\n\n")
print(f"Response from Model: {response.text}")

# gcloud config set project elasticsearch-on-gke
