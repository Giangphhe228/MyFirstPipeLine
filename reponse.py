from elasticsearch import Elasticsearch
from google.cloud import storage
from vertexai.language_models import TextEmbeddingModel, TextGenerationModel

model = TextEmbeddingModel.from_pretrained("textembedding-gecko-multilingual@latest")

# test case 1
# QUESTION = "tóm tắt các bước xử lý ram cao"
# keyword = "ram cao"

# test case 1.1
# QUESTION = "Thời gian giải quyết bước 2 khi xử lý ram cao"
# keyword = "ram cao"

# test case 1.2
# QUESTION = "khi xử lý ram cao nếu hệ thống hiển thị “System Idle Process” quá 45% thì sao?"
# keyword = "ram cao"

# test case 2
# QUESTION = "tóm tắt các bước khi xử lý trường hợp lỗi lastcheck"
# keyword = "lỗi lastcheck"

# test case 2.1
# QUESTION = "nêu chi tiết bước 2 khi xử lý trường hợp lỗi lastcheck"
# keyword = "lỗi lastcheck"

# test case 3: 
# QUESTION = "tóm tắt chi tiết các bước HDCV XỬ LÝ LỖI LAG CAO TRÊN EM"
# keyword = "Hướng dẫn công việc Xử ly lỗi LAG cao cho EM"

# QUESTION = "tóm tắt chi tiết các bước HDCV XỬ LÝ CẢNH BÁO CPU MÁY TÍNH CAO"
# keyword = "CPU MÁY TÍNH CAO"

# test case 4: format không thống nhất và phức tạp 
# QUESTION = "tóm tắt các bước HDCV KHÔI PHỤC DỊCH VỤ KHG KHÔNG TRUY CẬP ĐƯỢC ĐẾN SERVER"
# keyword = "KHÔNG TRUY CẬP ĐƯỢC ĐẾN SERVER"





# test case 5: 
# QUESTION = "tóm tắt chi tiết các bước KHÔI PHỤC DỊCH VỤ KHG TRUY CẬP ĐẾN SERVER CHẬM, RỚT GÓI"
# keyword = "KHÔI PHỤC DỊCH VỤ KHG TRUY CẬP ĐẾN SERVER CHẬM, RỚT GÓI"

# Connect to Elasticsearch
es = Elasticsearch(
    hosts=["http://localhost:9200"],
    basic_auth=("elastic", "Yv6qjBp8by754cujC7348M5h")
)

# Index name
index_name = 'work_flow_instruction_documents'

# Search for the nearest neighbors
embeddings = model.get_embeddings([keyword])
dims = len(embeddings)
# print("dims",dims)
script_query = {
    "script_score": {
        "query": {"match_all": { }},
        "script": {
            "source": "cosineSimilarity(params.query_vector, 'embedding') + 1.0",
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
print("source_blob_name",res["hits"]["hits"][0]['_id'])
# Download the text content of the file
text_content = download_blob(bucket_name, source_blob_name)

parameters = {
    "temperature": 0.2,
    "max_output_tokens": 1024,
    "top_p": 0.8,
    "top_k": 40
}
prompt = f"""
{text_content}

Nếu tài liệu bên trên hữu ích hãy sử dụng nó để trả lời câu hỏi: {QUESTION}
"""
model = TextGenerationModel.from_pretrained("text-bison@001")
response = model.predict(prompt, **parameters)

# print(f"Prompt: {prompt}\n\n")
print(f"Response from Model: {response.text}")

# gcloud config set project elasticsearch-on-gke
