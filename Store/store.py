'''
Author: error: error: git config user.name & please set dead value or install git && error: git config user.email & please set dead value or install git & please set dead value or install git
Date: 2023-07-25 12:43:47
LastEditors: xuhao0101
LastEditTime: 2023-09-15 14:25:25
FilePath: /Focus_Dataset_v2/store.py
Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置: https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
'''
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import time
import argparse
import json
import pickle
from tqdm import tqdm
from utils import *
from thread_read import multi_worker_read_text
from read_files_all import read_files
from itertools import chain

# 处理不同文件类型得数据，并保存为json，同时保存不同得信息
def main(args):
    # 初始化文件的信息
    file_info_path = os.path.join(args.save_path, "file_info.json")
    
    # 判断文件是否存在
    if not os.path.exists(file_info_path):
        # 创建一个新的json文件
        file_info = []
    else:
        # 读取已经存在的json文件
        file_info = read_json(file_info_path)
    
    # 初始化tokenizer
    tokenizer = get_tokenizer(args.tokenizer_path)
    # 是否读取文件信息
    if args.read_info: 
        # 读取文件夹下的所有文件，获取相关文件信息
        if args.mode == "init":
            base_info = {}
            # 处理初始化文件信息
            base_info = add_file_info(args.init_path, base_info, args.file_types)     
            # 将文件字典保存进字典中并保存
            file_info.append(base_info)
            write_json(file_info_path, file_info)
            print("read all init file done!")
        elif args.mode == "concat":
            base_info = file_info[0]
            for base_root, base_dirs, base_files in os.walk(args.concat_path):     
                for base_dir in base_dirs:
                    upload_path = os.path.join(base_root, base_dir)
                    # print("read {} start!".format(upload_path))
                    # 处理concat文件信息
                    base_info = add_file_info(upload_path, base_info, args.file_types)
            file_info = [base_info]
            write_json(file_info_path, file_info)
            print("read all concat file done!")
        print("files number: {}".format(len(file_info[0].keys())))
    
    if args.read_text:
        # 根据文件信息读取文件内容
        file_text_path = os.path.join(args.save_path, "file_text.json") # 存储文件的文本内容
        file_ids_path = os.path.join(args.save_path, "file_ids.json") # 已存储的文件id
        if os.path.exists(file_text_path):
            file_text_dict = read_json(file_text_path)[0]
            file_ids_dict = read_json(file_ids_path)
        else:
            file_text_dict = {}
            file_ids_dict = {"ids":[]}
        
        # 读取文件信息
        file_info = read_json(file_info_path)[0]
        file_ids= list(file_info.keys())
        
        # 过滤已经读取过的文件id
        update_ids = filter_ids(file_ids_dict[0]["ids"], file_ids)
        # 过滤不需要读取得文件id
        update_ids = filter_ids(file_ids_dict[1]["error_ids"], update_ids)
        
        # 根据文件id读取文件信息
        update_info = update_file_info(update_ids, file_info)
        # 批量根据文件地址读取文件内容
        
        if args.workers > 1:
            # 多线程读取文件内容
            inputs_info = []
            # file_texts = []
            for info in tqdm(list(update_info.keys())):
                inputs_info.append({info: update_info[info]})
            # 把列表中字典根据页数排序
            inputs_info = sorted(inputs_info, key=lambda x: list(x.values())[0]["file_pages"])
            # 把列表中字典根据类型排序
            inputs_info = sorted(inputs_info, key=lambda x: list(x.values())[0]["file_type"])
            file_texts = multi_worker_read_text(inputs_info[:args.file_numbers], args.workers)
            # print("MUL_end")
            # keys = list(update_info.keys())
            # for info in tqdm(keys[50:150]):
            #     inputs_info.append({info: update_info[info]})
            #     if len(inputs_info) == 20:
            #         f_texts = multi_worker_read_text(inputs_info, args.workers)
            # #         file_info, f_texts, file_text_dict, file_ids_dict = update_info(file_info, f_texts, file_text_dict, file_ids_dict, tokenizer, \
            # #    file_info_path, file_text_path, file_ids_path)
            #         file_texts.append(f_texts)
            #         inputs_info = []
            # file_texts = list(chain.from_iterable(file_texts))
            
        else:
            # 单线程读取文件内容
            file_texts = []
            keys = list(update_info.keys())
            i = 0
            for info in tqdm(keys[:20]):
            #     if i % 100:
            #         file_info, file_texts, file_text_dict, file_ids_dict = update_info(file_info, file_texts, file_text_dict, file_ids_dict, tokenizer, \
            #    file_info_path, file_text_path, file_ids_path)
                    
                file_text = read_file_text(update_info[info], info)
                file_texts.append({info: file_text})
            
        # 更新file_info的新字段
        for file in tqdm(file_texts):
            file_id = list(file.keys())[0]
            file_text = file[file_id]
            tokens_length = len(get_input_ids(file_text, tokenizer))
            file_info[file_id]["file_tokens"] = tokens_length
            file_text_dict[file_id] = file_text
            file_ids_dict[0]["ids"].append(file_id)
        write_json(file_info_path, [file_info])
        # 保存文件信息
        if not os.path.exists(file_text_path):
            # with open(file_text_path, "w", encoding="utf-8") as f:
            #     json.dump(file_text_dict, f)  
            write_json(file_text_path, [file_text_dict])
        else:
            write_json(file_text_path, [file_text_dict])
        write_json(file_ids_path, file_ids_dict)
    
    
    
    return 

# 读取文件内容
def read_file_text(file_info,file_id):
    file_type = file_info["file_type"]
    file_address = file_info["file_address"]
    file_language = file_info["file_language"]
    
    if file_type == "pdf":
        # print(file_id)
        # text, pages = pdf_extract_text_two(file_address, file_language)
        text = new_convert_pdf_to_text_with_split(file_address, file_id)
    elif file_type == "word":
        text, pages = docx_extract_text(file_address)
    else:
        text = read_files(file_address)
        # if file_language == "en":
        #     text, pages = pdf_extract_text_two(file_address, file_language)
        # elif file_language == "zh":
        #     text, pages = pdf_extract_text_one(file_address)
                
    return text
     
def update_info(file_info, file_texts, file_text_dict, file_ids_dict, tokenizer, \
               file_info_path, file_text_path, file_ids_path):
    # 更新file_info的新字段
    for file in file_texts:
        file_id = list(file.keys())[0]
        file_text = file[file_id]
        tokens_length = len(get_input_ids(file_text, tokenizer))
        file_info[file_id]["file_tokens"] = tokens_length
        file_text_dict[file_id] = file_text
        file_ids_dict["ids"].append(file_id)
    write_json(file_info_path, [file_info])
        # 保存文件信息
    if not os.path.exists(file_text_path):
            # with open(file_text_path, "w", encoding="utf-8") as f:
            #     json.dump(file_text_dict, f)  
        write_json(file_text_path, [file_text_dict])
    else:
        write_json(file_text_path, [file_text_dict])
    write_json(file_ids_path, [file_ids_dict])
    return file_info, file_texts, file_text_dict, file_ids_dict 


def add_file_info(path, base_info, file_types):
    try:
        for root, dirs, f in os.walk(path):
            for dir in dirs:
                # language = get_language(dir)
                dir_path = os.path.join(root, dir)
                print("read {} start!".format(dir_path))
                # 获取文件名称
                files = os.listdir(dir_path)
                for file in tqdm(files):
                    # 获取文件名称与类型
                    filename, suffix = get_filename_suffix(file)
                    if is_contain_chinese(filename):
                        language = "zh"
                    else:
                        language = "en"
                    file_type = get_file_type(suffix)
                    # 不需要读取的类型，直接pass
                    if file_type not in file_types:
                        continue
                    else:
                        # 根据文件类型过滤文件信息
                        file_dict = get_file_info(file_type, base_info)
                        filenames = list(file_dict.keys())
                        # 新文件的地址
                        file_path = os.path.join(dir_path, file)
                        # 对比文件名称
                        is_same_file= is_repeat_filename(filenames, filename)
                        # 新文件的页数
                        file_page = get_file_pages(file_type, file_path)
                        
                        if is_same_file[0] == 1:
                            for name in list(file_dict.keys()):
                                if name == is_same_file[1]:
                                    pages = file_dict[name]["file_pages"]
                                    if abs(file_page - pages) <= 2:
                                        print("file {} is same file, pass!".format(file_path))
                                        continue
                        else:
                            """添加新的文件信息
                            1. 获取文件id
                            2. 添加文件标题
                            3. 添加文件地址
                            4. 添加文件页数
                            """
                            file_ids = list(file_dict.keys())
                            # 获取新文件的id
                            while True:
                                file_id = get_file_id()
                                if file_id not in file_ids:
                                    break
                            
                            # 新文件的标题
                            file_title = is_same_file[1]
                                
                            # 添加新文件信息
                            base_info[file_id] = {
                                    "file_title" : file_title,
                                    "file_type" : file_type,
                                    "file_address" : file_path,
                                    "file_pages" : file_page,
                                    "file_language" : language,
                                    "file_dir" : dir
                                }
                print("read {} done!".format(dir_path))
    except:
        pass
    return base_info

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--init_path", type=str, default="/home/xuhao/xcxhy/Focus_Dataset/books_papers")
    parser.add_argument("--concat_path", type=str, default="/home/xuhao/xcxhy/Focus_Dataset/new_books_papers")
    parser.add_argument("--save_path", type=str, default="/home/xuhao/xcxhy/Focus_Dataset_v2/trade_information")
    parser.add_argument("--tokenizer_path", type=str, default="/home/xuhao/xcxhy/Focus_Dataset/llama2_tokenizer/llama2-7b-hf")
    parser.add_argument("--file_types", type=list, choices=["pdf", "word", "doc", "txt","epub", "xlsx", "csv", "ppt"], default=["word"])
    parser.add_argument("--mode", type=str, choices=["init", "concat"], default="concat")
    parser.add_argument("--file_numbers", type=int, default=-1)
    parser.add_argument("--read_text", type=bool, default=True)
    parser.add_argument("--read_info", type=bool, default=False)
    parser.add_argument("--workers", type=int, default=1)
    args = parser.parse_args()
    main(args)
    