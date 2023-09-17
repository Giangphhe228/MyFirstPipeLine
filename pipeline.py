import kfp
import kfp.dsl as dsl

from kfp import compiler
from kfp.dsl import Dataset, Input, Output

from typing import Dict, List



@dsl.component(
    base_image='python:3.11',
    packages_to_install=['appengine-python-standard']
)
def get_matching_files(directory: str, pattern: str) -> List[str]:
    import os
    import re

    # Compile the regex pattern
    regex = re.compile(pattern)
    # List to store matching file paths
    matching_files = []
    print("directory là:",directory)
    # Walk through directory including subdirectories
    for root, dirs, files in os.walk(directory.replace("gs://", "/gcs/")):
        for file in files:
            # If file name matches the pattern, add it to the list
            if regex.match(os.path.join(root, file).replace(directory.replace("gs://", "/gcs/"), "")):
                if "-cid" not in os.path.join(root, file):
                    # os.path.join concatenates root, dirs, and file into a full path
                    matching_files.append(os.path.join(root, file))
    
    print("matching_files là:",matching_files)
    # Return the list of matching files
    return [matching_file.replace("/gcs/", "gs://") for matching_file in matching_files]

@dsl.component(
    base_image='python:3.11',
    packages_to_install=['pypdf2==2.12.1', 'appengine-python-standard']
)
def split_pdf_into_pages(pdf_file: str) -> List[str]:
    import os
    import PyPDF2

    page_files = []

    # Open the PDF file
    with open(pdf_file.replace("gs://", "/gcs/"), 'rb') as file:
        # Create a PDF reader object
        pdf_reader = PyPDF2.PdfFileReader(file)

        # Get the total number of pages in the PDF
        total_pages = pdf_reader.numPages

        # Iterate through each page and save it as a separate PDF
        for page_number in range(total_pages):
            # Get a page
            pdf_page = pdf_reader.getPage(page_number)

            # Create a PDF writer object
            pdf_writer = PyPDF2.PdfFileWriter()

            # Add the page to the writer
            pdf_writer.addPage(pdf_page)

            # Output file name
            output_file_path = pdf_file.replace("gs://", "/gcs/").replace(".pdf", "/") + \
                pdf_file.split("/")[-1].replace(".pdf", f".{page_number + 1}.pdf")

            # Create the directory if it doesn't exist
            os.makedirs(
                os.path.dirname(pdf_file.replace("gs://", "/gcs/").replace(".pdf", "/")),
                exist_ok=True
            )

            # Save the page as a PDF file
            with open(output_file_path, 'wb') as output_file:
                pdf_writer.write(output_file)
            page_files.append(output_file_path.replace("/gcs/", "gs://"))

    print(f'Successfully split the PDF into {total_pages} pages')
    return page_files

@dsl.component(
    base_image='python:3.11',
    packages_to_install=['PyMuPDF==1.23.3', 'appengine-python-standard']
)
def clear_image_pdf(pdf_file: str) -> str:
    import fitz
    pdf_document = fitz.open(pdf_file.replace("gs://", "/gcs/"))
    for page in pdf_document:
            img_list = page.get_images()
            for img in img_list:
                page.delete_image(img[0])
    output_txt_path = pdf_file.replace("gs://", "/gcs/").replace(".docx", "").replace(".pdf", "-cid.pdf")  
    pdf_document.save(output_txt_path)
    pdf_document.close()
    print("output_txt_path là:",output_txt_path)
    return output_txt_path.replace("/gcs/", "gs://")


@dsl.component(
    base_image='python:3.11',
    packages_to_install=['google-cloud-documentai', 'appengine-python-standard']
)
def parse_text(pdf_file_path: str) -> str:
    from google.cloud import documentai
    from google.api_core.client_options import ClientOptions
    import re

    print("pdf_file_path path là:",pdf_file_path)
    project_id = '738762457831'
    location = 'us'
    mime_type = 'application/pdf'
    processor_id = 'c6e90180ec966c2d'
    opts = ClientOptions(api_endpoint=f"{location}-documentai.googleapis.com")
    client = documentai.DocumentProcessorServiceClient(client_options=opts)
    name = client.processor_path(project_id, location, processor_id)
    with open(pdf_file_path.replace("gs://", "/gcs/"), "rb") as image:
        image_content = image.read()
    raw_document = documentai.RawDocument(content=image_content, mime_type=mime_type)
    request = documentai.ProcessRequest(name=name, raw_document=raw_document)
    result = client.process_document(request=request)
    document = result.document
    info_string = document.text
    lines = info_string.split('\n')
    # Tạo danh sách mới để lưu trữ các dòng đã lọc
    cleaned_lines = []
    for line in lines:
        # Kiểm tra nếu dòng không chứa "HDCV/" và "Lần ban hành/sửa đổi:"
        # và không phải dòng chỉ có một ký tự
        
        if "HDCV/" not in line and "Lần ban hành/sửa đổi:" not in line and len(line.strip()) > 1:
            # Thêm dòng này vào danh sách đã lọc
            new_line=re.sub(r'\s+', ' ', line)
            cleaned_lines.append(new_line)
    # Gắn lại các dòng đã lọc để tạo thành một chuỗi mới
    cleaned_text = '\n'.join(cleaned_lines)
    # Sử dụng regex để loại bỏ các ký tự không mong muốn và thay thế dấu gạch ngang bằng dấu cách
    # filename = pdf_file_path.split('/')[-1].split('.')[0].replace('-', ' ')
    # cleaned_filename = re.sub(r'\s+', ' ', filename)
    # cleaned_filename = re.sub(r'[^\w\sÀ-ỹ]', '', cleaned_filename)
    # cleaned_text=   cleaned_filename + '\n' + cleaned_text
    print(f'pdf_file_path name: {pdf_file_path} pages')
    with open(pdf_file_path.replace("gs://", "/gcs/").replace(".pdf", ".txt"), 'w') as file:
        file.write(cleaned_text)
    return pdf_file_path.replace("/gcs/", "gs://").replace(".pdf", ".txt")

@dsl.component(
    base_image='python:3.11',
    packages_to_install=['google-cloud-aiplatform', 'appengine-python-standard']
)
def generate_embedding(txt_file: str) -> Dict:
    from vertexai.language_models import TextEmbeddingModel

    model = TextEmbeddingModel.from_pretrained("textembedding-gecko@001")

    with open(txt_file.replace("gs://", "/gcs/"), 'r') as f:
        text = f.read()
        embeddings = model.get_embeddings([text])
        embedding = embeddings[0].values
        
    print("đây là embedding:\n",embedding) 

    return {"id": txt_file, "embedding": embedding}

@dsl.component(
    base_image='python:3.11',
    packages_to_install=['elasticsearch', 'appengine-python-standard']
)
def write_embeddings(embedding: Dict):
    from elasticsearch import Elasticsearch
    # kubectl get secret quickstart-es-elastic-user -o yaml
    # echo "WXY2cWpCcDhieTc1NGN1akM3MzQ4TTVo" | base64 -d
    # Connect to the Elasticsearch instance
    es= None
    es = Elasticsearch(
            hosts=["http://10.128.0.62:9200"],        
            basic_auth=("elastic", "Yv6qjBp8by754cujC7348M5h")
        )                         
        
    # es = Elasticsearch([{'host': 'http://10.128.0.6:9200', 'port': '9200', 'basic_auth': 'WXY2cWpCcDhieTc1NGN1akM3MzQ4TTVo'}], timeout=30, max_retries=10, retry_on_timeout=True)
    # Name of the index
    index_name = "work_flow_instruction_documents"
    # Define the mapping for the index
    mapping = {
        "mappings": {
            "properties": {
                "embedding": {
                    "type": "dense_vector",
                    "dims": 768
                }
            }
        }
    }
    print("đây là elasticsearch:\n",es)  
    # Create the index with the mapping
    cluster_health = es.cluster.health()
    print("đây là cluster_health:\n",cluster_health)
    node_stats = es.nodes.stats()
    print("đây là node_stats:\n",node_stats)
    es.indices.create(index=index_name, ignore=400, body=mapping)
    # Index the vector embeddings
    es.index(index=index_name, id=embedding["id"], body={"embedding": embedding["embedding"]})

    print("Embeddings indexed successfully.")


@dsl.pipeline(
    name="work-flow-instruction-documents",
)
def work_flow_instruction_documents(gcs_directory: str):
    get_matching_files_task = get_matching_files(
        directory=gcs_directory,
        pattern="^[^/]*\.pdf"
    )
    with dsl.ParallelFor(
        name="pdf-parsing",
        items=get_matching_files_task.output,
        parallelism=3
    ) as pdf_file:
            # split_pdf_into_pages_task = split_pdf_into_pages(
            #     pdf_file=pdf_file
            # )
        # with dsl.ParallelFor(
        #     name="pdf-page-parsing",
        #     items=split_pdf_into_pages_task.output,
        #     parallelism=3
        # ) as pdf_page_file:
            clear_image_task = clear_image_pdf(
                pdf_file=pdf_file
            )
            parse_text_task = parse_text(
                pdf_file_path=clear_image_task.output
            )
            # clean_task = clean_noise(
            #     document_content= parse_text_task.output,pdf_file_path=clear_image_task.output
            # )
            generate_embedding_task = generate_embedding(
                txt_file=parse_text_task.output
            )
            write_embeddings_task = write_embeddings(
                embedding=generate_embedding_task.output
            )


compiler.Compiler().compile(work_flow_instruction_documents, 'pipeline.json')
