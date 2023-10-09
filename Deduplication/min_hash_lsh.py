import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import numpy as np 
import time
from utils import *
import re
from tqdm import tqdm
from itertools import chain, repeat
from datasketch import MinHash, MinHashLSH
import jieba
import pickle
from concurrent import futures
from multiprocessing import Pool


def compute_minhash(id, text, minhashes, lsh):
    minhash = MinHash(num_perm=256)
    text = re.sub("([^\u0030-\u0039\u4e00-\u9fa5\u0041-\u007a])", '', text)
    if is_contain_chinese(text):
        words = jieba.cut(text)
    else:
        words = text.split()
    for word in words:
        minhash.update(word.encode("utf-8"))
    minhashes[id] = minhash
    lsh.insert(id, minhash)
    return minhashes, lsh
def minhash_lsh(minhashes, lsh):
    for key in list(minhashes.keys()):
        lsh.insert(key, minhashes[key])
    return lsh
def compute_min(id, text):
    minhash = MinHash(num_perm=256)
    text = re.sub("([^\u0030-\u0039\u4e00-\u9fa5\u0041-\u007a])", '', text)
    if is_contain_chinese(text):
        words = jieba.cut(text)
    else:
        words = text.split()
    for word in words:
        minhash.update(word.encode("utf-8"))
    return (id, minhash)

def multi_compute_minhash(id, text, minhashes, lsh):
    id, minhash = compute_min(id, text)
    minhashes[id] = minhash
    lsh.insert(id, minhash)
    return minhashes, lsh

def new_check_duplicate(id, minhashes, lsh):
    result = lsh.query(minhashes[id])
    result = sorted(result)
    if len(result) > 1:
        return result
    else:
        return None
def search_duplicate(id, minhashes, lsh):
    result = lsh.query(minhashes[id])
    result = sorted(result)
    if len(result) > 0:
        return {id: result}
    else:
        return None
    
def hash_store(id_to_text_dict, processing="single1", workers=4):
    minhashes = {}
    lsh = MinHashLSH(num_perm=256, threshold=0.9)
    if processing == "single":
        start = time.time()
        for key in tqdm(list(id_to_text_dict.keys())):
            minhashes, lsh = compute_minhash(key, id_to_text_dict[key], minhashes, lsh)
        end = time.time()
        print("time: ", end - start)
    else:
        start = time.time()
        ids = list(id_to_text_dict.keys())
        texts = [value for value in id_to_text_dict.values()]
        pool = Pool(workers)
        chunk_size = len(ids)//pool._processes
        result = pool.starmap(compute_min, zip(ids, texts), chunksize=chunk_size)
        pool.close()
        pool.join()
        # with futures.ThreadPoolExecutor(10) as executor:
        #     result = executor.map(compute_min, ids, texts)
        for value in tqdm(result):
            id = value[0]
            minhash = value[1]
            minhashes[id] = minhash
            lsh.insert(id, minhash)
        end = time.time()
        print("time: ", end - start)

    duplicated_result = []
    for key in tqdm(list(id_to_text_dict.keys())):
        result = new_check_duplicate(key, minhashes, lsh)
        if result:
            duplicated_result.append(result)   
        # search_pool = Pool(10)
        # chunk_size = len(list(minhashes.keys()))//search_pool._processes
        # res = search_pool.starmap(new_check_duplicate, zip(list(minhashes.keys()), repeat(minhashes),repeat(lsh)), chunksize=chunk_size)
        # res = list(res)
        # pool.close()
        # pool.join()
        # duplicated_result = [value for value in res if value is not None]
        # end = time.time()
        # print("time: ", end - start)
    start = time.time()
    merged_list = merge_lists_with_intersection(duplicated_result)
    end = time.time()
    print("merge_time: ", end-start)
    all_merged_list = list(chain.from_iterable(merged_list))
    new_id_to_text_dict = {}
    for id in tqdm(list(id_to_text_dict.keys())):
        if id in set(all_merged_list):
            continue
        else:
            new_id_to_text_dict[id] = id_to_text_dict[id]
    new_minhashes = {}
    for id in list(new_id_to_text_dict.keys()):
        new_minhashes[id] = minhashes[id]
    # 加入重复
    # duplicated_res = {}
    # for values in merged_list:
    #     if values[0] in duplicated_res.keys():
    #         duplicated_res[values[0]].extend(values[1:])
    #     else:
    #         duplicated_res[values[0]] = values[1:]
    duplicated_res = choose_duplicate_ids(merged_list)

    for key in list(duplicated_res.keys()):
        new_minhashes[key] = minhashes[key]
    
    return new_minhashes, duplicated_res

def hash_search(id_to_text_dict, minhashes, processing="single", workers=4):
    search_minhashes = {}
    origin_lsh = MinHashLSH(num_perm=256, threshold=0.9)
    origin_lsh = minhash_lsh(minhashes, origin_lsh)
    if processing == "single":
        search_lsh = MinHashLSH(num_perm=256, threshold=0.9)
        for key in tqdm(list(id_to_text_dict.keys())):
            search_minhashes, search_lsh = compute_minhash(key, id_to_text_dict[key], search_minhashes, search_lsh)
    else:
        start = time.time()
        search_lsh = MinHashLSH(num_perm=256, threshold=0.9)
        ids = list(id_to_text_dict.keys())
        texts = [value for value in id_to_text_dict.values()]
        pool = Pool(workers)
        chunk_size = len(ids)//pool._processes
        result = pool.starmap(compute_min, zip(ids, texts), chunksize=chunk_size)
        pool.close()
        pool.join()
        
        for value in tqdm(result):
            id = value[0]
            minhash = value[1]
            search_minhashes[id] = minhash
            search_lsh.insert(id, minhash)
        end = time.time()
        print("time: ", end - start)
    duplicated_result = []
    for key in tqdm(list(id_to_text_dict.keys())):
        result = search_duplicate(key, search_minhashes, origin_lsh)
        if result:
            duplicated_result.append(result)
        
    
    duplicated_ids = []
    for value in duplicated_result:
        for key in value:
            duplicated_ids.append(key)
            
    deduplicated_minhashes = {}
    for key in tqdm(list(search_minhashes.keys())):
        if key in duplicated_ids:
            continue
        else:
            deduplicated_minhashes[key] = search_minhashes[key]
    
    # 合并两个字典
    # for id in duplicated_ids:
    #     if id in list(minhashes.keys()):
    #         print("error") 
    return deduplicated_minhashes, duplicated_result
def multi_get_minhash(id_to_text_dict, processing="single", workers=4):
    search_minhashes = {}
    if processing == "single":
        search_lsh = MinHashLSH(num_perm=256, threshold=0.9)
        for key in tqdm(list(id_to_text_dict.keys())):
            search_minhashes, search_lsh = compute_minhash(key, id_to_text_dict[key], search_minhashes, search_lsh)
    else:
        start = time.time()
        search_lsh = MinHashLSH(num_perm=256, threshold=0.9)
        ids = list(id_to_text_dict.keys())
        texts = [value for value in id_to_text_dict.values()]
        pool = Pool(workers)
        chunk_size = len(ids)//pool._processes
        result = pool.starmap(compute_min, zip(ids, texts), chunksize=chunk_size)
        pool.close()
        pool.join()
        
        for value in tqdm(result):
            id = value[0]
            minhash = value[1]
            search_minhashes[id] = minhash
            search_lsh.insert(id, minhash)
        end = time.time()
        print("time: ", end - start)
    return search_minhashes

def multi_hash_search(search_minhashes, minhashes):
    origin_lsh = MinHashLSH(num_perm=256, threshold=0.9)
    origin_lsh = minhash_lsh(minhashes, origin_lsh)
    
    duplicated_result = []
    for key in tqdm(list(search_minhashes.keys())):
        result = search_duplicate(key, search_minhashes, origin_lsh)
        if result:
            duplicated_result.append(result)
    
    duplicated_ids = []
    for value in duplicated_result:
        for key in value:
            duplicated_ids.append(key)
    
    deduplicated_minhashes = {}
    for key in tqdm(list(search_minhashes.keys())):
        if key in duplicated_ids:
            continue
        else:
            deduplicated_minhashes[key] = search_minhashes[key]
    return deduplicated_minhashes, duplicated_result

# 从重复ids中选出一个id，加入到新的字典中
def choose_duplicate_ids(duplicated_list):
    duplicated_res = {}
    for values in duplicated_list:
        if values[0] in duplicated_res.keys():
            duplicated_res[values[0]].extend(values[1:])
        else:
            duplicated_res[values[0]] = values[1:]
    return duplicated_res

if __name__=="__main__":
    data_path = "/hdfs_nfs_mount/LLM_data/llm_basedata/process/unique/networks/trade_networks_2023_08_23_unique.json"
    data = read_json(data_path)
    
    id_to_text_dict, id_to_value_dict = {}, {}
    for i in tqdm(range(10000)):
        value = data[i]
        id_to_text_dict[value["unique_id"]] = value["text"]
        id_to_value_dict[value["unique_id"]] = value
    minhashes, merged_list = hash_store(id_to_text_dict)
    print(len(merged_list))
    # store_minhashes("/home/xuhao/xcxhy/Focus_Hash/dataset/minhashes.json", minhashes)
    
    # minhashes = read_minhashes("/home/xuhao/xcxhy/Focus_Hash/dataset/minhashes.json")
    # new_minhashes, duplicated_ids = hash_search(id_to_text_dict, minhashes)
    
    cate = "networks"
    hashindex_dir = "/hdfs_nfs_mount/LLM_data/llm_basedata/process/hash_index"
    single_hashindex_dir = os.path.join(hashindex_dir, cate)
    if not os.path.exists(single_hashindex_dir):
        os.makedirs(single_hashindex_dir)
        
    if len(list(minhashes.keys())) > 2000:
        parts_minhashes = []
        for i in range(0, len(list(minhashes.keys())), 2000):
            parts_minhashes.append(dict(list(minhashes.items())[i:i+2000]))
        for i in range(len(parts_minhashes)):
            parts = parts_minhashes[i]
            store_minhashes(os.path.join(single_hashindex_dir, "{cate}_minhashes_{num}.json".format(cate=cate, num=i)), parts)
    else:
        store_minhashes(os.path.join(single_hashindex_dir, "{}_minhashes.json".format(cate)), minhashes)
    
    