import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from concurrent import futures
from utils import *
from read_files_all import read_files
from itertools import chain, repeat


def multi_worker_read_text(info, workers):
    test_list = []
    for file_info in info:
        file_id = list(file_info.keys())[0]
        file_type = file_info[file_id]["file_type"]
        file_address = file_info[file_id]["file_address"]
        file_language = file_info[file_id]["file_language"]
        test_list.append((file_id, file_type, file_address, file_language))
    with futures.ThreadPoolExecutor(workers) as executor:
        res = executor.map(read_file_text, test_list)
        
        res = list(res)
        new_list = []
        for k,v in res:
            new_list.append({k:v})
        # print("MMMMM")
        return new_list

def multi_worker_split_text(info, tokenizer, slide_length, workers):
    with futures.ThreadPoolExecutor(workers) as executor:
        res = executor.map(slide_text, info, repeat(tokenizer), repeat(slide_length))
        res = list(res)
        res = list(chain.from_iterable(res))
        
        return res
# 读取文件内容
def read_file_text(test):
    file_id = test[0]
    file_type = test[1]
    file_address = test[2]
    file_language = test[3]
    
    if file_type == "pdf":
        # print(file_id)
        text = new_convert_pdf_to_text_with_split(file_address, file_id)
    elif file_type == "word":
        text, pages = docx_extract_text(file_address)
    else:
        text = read_files(file_address)
        # if file_language == "en":
        #     text, pages = pdf_extract_text_two(file_address, file_language)
        # elif file_language == "zh":
        #     text, pages = pdf_extract_text_one(file_address)
    return (file_id, text)

def slide_text(file, tokenizer, slide_length):
    output = []
    id = list(file.keys())[0]
    values = file[id]
    
    info = values[0]
    text = values[1]
    
    language = info["file_language"]
    tokens = info["file_tokens"]
    
    if tokens >= slide_length:
        if language == "zh":
            sep = "。"
        elif language == "en":
            sep = "."
        text_list, origin_length, final_length = slide_window(text, slide_length, tokenizer, sep)
        for t in text_list:
            output.append({
                "text" : t,
                "meta" :{"id" : id,
                         "language" : language}
            })
    else:
        output.append({
            "text" : text,
            "meta" :{"id" : id,
                     "language" : language}
        })
    return output
    