import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import numpy as np 
from utils import *
import re
from tqdm import tqdm
from itertools import chain
from datasketch import MinHash, MinHashLSH
import jieba

lsh = MinHashLSH(num_perm=256, threshold=0.9)
minhashes = {}

def compute_minhash(id, text):
    minhash = MinHash(num_perm=256)
    if is_contain_chinese(text):
        words = jieba.cut(text)
    else:
        words = text.split()
    for word in words:
        minhash.update(word.encode("utf-8"))
    minhashes[id] = minhash
    lsh.insert(id, minhash)
    

def check_duplicate(n):
    value_dict = {}
    all_duplicated = []
    for index in range(n):
        value_dict[index] = 1
    for index in range(n):
        if value_dict[index]:
            results = lsh.query(minhashes[index])
            results =sorted(results)
            if len(results) > 1:
                all_duplicated.append(results)
        else:
            continue
    return all_duplicated

def min_hashlsh_main(text_list):
    members = []
    for idx, i in enumerate(tqdm(text_list)):
        members.append(i)
        compute_minhash(idx, i)
    print("END TO COMPUTE MINHASHES")
    merge_list = check_duplicate(len(text_list))
    
    merged_list = merge_list_with_intersection(merge_list)
    
    text_dict = {}
    for merged in merged_list:
        texts = []
        for index in merged:
            texts.append(text_list[index])
        text_dict[str(merged)] = texts
    return merged_list, text_dict
            

def merge_list_with_intersection(lists):
    merged = []
    for sublist in tqdm(lists):
        sublist = sorted(sublist)
        if len(merged) == 0:
            merged.append(sublist)
            continue
        elif sublist in merged:
            continue
        for i in range(len(merged)):
            merge = set(merged[i])
            if set(sublist).intersection(merge):
                merge_list = list(merge)
                merge_list.extend(sublist)                
                merge = list(set(merge_list))
                merged[i] = sorted(merge)
                break
            elif set(sublist).intersection(merge) != 1 and i == len(merged) - 1:
                merged.append(sublist)
    return merged
        
        
        
    
    
    

# if __name__=="__main__":
#     start_time = time.time()
#     data = read_json("/home/xuhao/xcxhy/Focus_Dataset/data/parts/foreign_trade_08_23_merge_content_clean_100.json")
#     text_list, text_dict = [], {}
#     for i in tqdm(range(len(data))):
#         value = data[i]
#         text_dict[value["text"]] = value
#         text_list.append(value["text"])
    
#     text_list = list(set(text_list))
#     if "" in text_list:
#         text_list.remove("")
#     if " " in text_list:
#         text_list.remove(" ")
#     print("nums: ", len(text_list))
        
#     new_data = []
#     for text in text_list:
#         new_data.append(text_dict[text])
#     print(len(text_list))
#     res_dict, text_dict = min_hashlsh_main(text_list)
#     end_time = time.time()
#     print("time: ", end_time - start_time)
#     write_json("/home/xuhao/xcxhy/Focus_Hash/dataset/test/foreign_trade_08_23_merge_content_clean_100_minhash_result.json", text_dict)
#     write_json("/home/xuhao/xcxhy/Focus_Hash/dataset/test/foreign_trade_08_23_merge_content_clean_100_minhash_index.json", res_dict)
    