import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import argparse
import re
from itertools import chain, repeat
from concurrent import futures
from utils import *
from Deduplication.min_hash_lsh import hash_store, hash_search, multi_get_minhash, multi_hash_search

def main(args):
    # 读取数据
    # 判断是文件还是文件夹
    if os.path.isfile(args.data_path):
        data = read_json(args.data_path)
        data = data[:100000]
        cate = choose_file_category(args.data_path)
    elif os.path.isdir(args.data_path):
        data = []
        files = os.listdir(args.data_path)
        for file in tqdm(files):
            if file.endswith(".json") or file.endswith(".jsonl"):
                file_path = os.path.join(args.data_path, file)
                value = read_json(file_path)
                data.extend(value)
        cate = choose_file_category(os.path.join(args.data_path, files[0]))
        data = data[:1000000]
    else:
        raise ValueError("data path is not exist")
    
    # 读取id-text字典，id-value字典
    id_to_text_dict, id_to_value_dict = {}, {}
    for i in tqdm(range(len(data))):
        value = data[i]
        id_to_text_dict[value["unique_id"]] = value["text"]
        id_to_value_dict[value["unique_id"]] = value
    
    # 绝对去重
    set_choosed_ids = get_set_deduplicated(id_to_text_dict, cate)
    
    # 根据ids缩减字典
    set_id_to_text_dict = choose_set_ids(set_choosed_ids, id_to_text_dict, cate)
    
    # hash去重
    if args.use_hash:
        minhashes = single_minhash_deduplicated(mode=args.deduplicate_mode, id_to_text_dict=set_id_to_text_dict, \
        cate=cate, processing=args.processing, workers=args.workers, name=args.name, \
            hashindex_dir=args.hashindex_dir, deduplicated_ids_dir=args.deduplicated_ids_dir)
        print("minhashes: ", len(list(minhashes.keys())))
    else:
        hashed_choosed_ids = set_choosed_ids
    
    
    
        
    
    return 

# 对数据进行绝对去重
def get_set_deduplicated(id_to_text_dict, cate):
    choosed_ids = {} # 保存去重后的ids
    set_id_to_text_dict = new_set_deduplicated(id_to_text_dict)
    choosed_ids[cate] = list(set_id_to_text_dict.keys())
    # 保存去重后的ids
    return choosed_ids[cate]
# 新版本绝对去重算法
def new_set_deduplicated(data_dict):
    text_list, key_list = [], []
    for key in tqdm(list(data_dict.keys())):
        text_list.append(data_dict[key])
        key_list.append(key)
    print("未去重文本数量：", len(text_list))
    text_to_index_dict = dict(zip(text_list, key_list)) # 文本与index得字典，方便后续查找
    # 去除绝对相同
    set_list = list(set(text_list))
    if "" in set_list:
        set_list.remove("")
    if " " in set_list:
        set_list.remove(" ")
    print("去除绝对重复与空值文本数量：", len(set_list))
    # 建立去除绝对重复后得index-text字典
    set_dict = {}
    for set_text in tqdm(set_list):
        set_dict[text_to_index_dict[set_text]] = set_text
    return set_dict
# 根据ids缩减字典
def choose_set_ids(ids_list, id_to_text_dict, cate):
    choosed_set_id_to_text_dict = {}
    for id in ids_list:
        choosed_set_id_to_text_dict[id] = id_to_text_dict[id]
    return choosed_set_id_to_text_dict

# 保存minhashes
def write_minhashes_index(minhash_dir, cate, minhashes):
    single_hashindex_dir = os.path.join(minhash_dir, cate)
    if not os.path.exists(single_hashindex_dir):
        os.makedirs(single_hashindex_dir)
    else:
        shutil.rmtree(single_hashindex_dir)
        os.mkdir(single_hashindex_dir)
    if len(list(minhashes.keys())) > 1000000:
        parts_minhashes = []
        for i in range(0, len(list(minhashes.keys())), 1000000):
            parts_minhashes.append(dict(list(minhashes.items())[i:i+1000000]))
        for i in range(len(parts_minhashes)):
            parts = parts_minhashes[i]
            store_minhashes(os.path.join(single_hashindex_dir, "{cate}_minhashes_{num}.json".format(cate=cate, num=i)), parts)
    else:
        store_minhashes(os.path.join(single_hashindex_dir, "{}_minhashes.json".format(cate)), minhashes)
    return 
# 保存部分minhashes
def write_part_minhashes_index(minhash_dir, cate, name, minhashes):
    single_hashindex_dir = os.path.join(minhash_dir, cate)
    if not os.path.exists(single_hashindex_dir):
        os.makedirs(single_hashindex_dir)
    if len(list(minhashes.keys())) > 1000000:
        parts_minhashes = []
        for i in range(0, len(list(minhashes.keys())), 1000000):
            parts_minhashes.append(dict(list(minhashes.items())[i:i+1000000]))
        for i in range(len(parts_minhashes)):
            parts = parts_minhashes[i]
            store_minhashes(os.path.join(single_hashindex_dir, "{cate}_minhashes_{name}_{num}.json".format(cate=cate, name=name, num=i)), parts)
    else:
        store_minhashes(os.path.join(single_hashindex_dir, "{cate}_minhashes_{name}.json".format(cate=cate, name=name)), minhashes)
    return 
# 读取minhashes
def read_minhashes_index(minhash_dir, cate):
    if os.path.exists(os.path.join(minhash_dir, cate)):
        filename = os.listdir(os.path.join(minhash_dir, cate))
    else:
        raise ValueError("minhashes path is not exist {}".format(os.path.join(minhash_dir, cate)))
    
    minhashes = {}
    for file in filename:
        if file.endswith(".json"):
            file_path = os.path.join(minhash_dir, cate, file)
            value = read_minhashes(file_path)
            minhashes.update(value)
    return minhashes
# 读取部分minhashes索引
def read_part_minhashes_index(minhash_dir, cate, is_file, max_files=2):
    filename = os.listdir(os.path.join(minhash_dir, cate))
    nums = 0
    minhashes = {}
    for file in filename:
        if file in is_file:
            continue
        if nums > max_files:
            return minhashes, is_file
        if file.endswith(".json") :
            file_path = os.path.join(minhash_dir, cate, file)
            value = read_minhashes(file_path)
            minhashes.update(value)
            nums += 1
            is_file.append(file)
    return minhashes, is_file
# 合并两个字典
def dict_update_intersection(dict1, dict2):
    new_dict = {}
    if dict1 == {}: 
        return dict2
    else:
        intersection = dict1.keys() & dict2.keys()
        
    for key in intersection:
        new_dict[key] = dict1[key]
    return new_dict
# 单个类型文本去重
def single_minhash_deduplicated(mode, id_to_text_dict, cate, processing, workers, name, hashindex_dir, deduplicated_ids_dir):
    print("your choose {} mode".format(mode))
    if mode == "store":
        minhashes, duplicated_ids = hash_store(id_to_text_dict, processing, workers)
        print("minhashes: ", len(list(minhashes.keys())))
        # 保存minhashes
        write_minhashes_index(hashindex_dir, cate, minhashes)
        print("deduplicated minhashes finished!")
        # 保存去重后的ids
        if os.path.exists(os.path.join(deduplicated_ids_dir, "deduplicated_ids.json")):
            os.remove(os.path.join(deduplicated_ids_dir, "deduplicated_ids.json"))
        write_json(os.path.join(deduplicated_ids_dir, "deduplicated_ids.json"), {cate: list(minhashes.keys())})
        print("write deduplicated ids finished!")
        # 保存重复的字典
        if os.path.exists(deduplicated_ids_dir):
            write_json(os.path.join(deduplicated_ids_dir, "duplicated_ids.json"), {cate: duplicated_ids})
        print("write duplicated ids finished!")
        return minhashes
    elif mode == "search":
        # 读取minhashes
        start = time.time()
        minhashes = read_minhashes_index(hashindex_dir, cate)
        print("origin minhashes length: ", len(list(minhashes.keys())))
        new_minhashes, duplicated_ids = hash_search(id_to_text_dict, minhashes, processing, workers)
        print("new minhashes length: ", len(list(new_minhashes.keys())))
        # 是否合并两个字典
        minhashes.update(new_minhashes)
        end = time.time()
        print("search speed ", end-start)
        # return 
        # 保存minhashes
        write_minhashes_index(hashindex_dir, cate, minhashes)
        # 保存去重后的ids
        print("deduplicated minhashes finished!")
        os.remove(os.path.join(deduplicated_ids_dir, "deduplicated_ids.json"))
        write_json(os.path.join(deduplicated_ids_dir, "deduplicated_ids.json"), {cate: list(minhashes.keys())})
        print("write deduplicated ids finished!")
        # 保存重复的字典
        if os.path.exists(os.path.join(deduplicated_ids_dir, "duplicated_ids.json")):
            duplicated_dict = read_json(os.path.join(deduplicated_ids_dir, "duplicated_ids.json"))
            cate_duplicated_dict= duplicated_dict[cate]
            origin_duplicated_keys = list(cate_duplicated_dict.keys())
            for value in duplicated_ids:
                for k, v in value.items():
                    if k in origin_duplicated_keys:
                        cate_duplicated_dict[k].extend(v)
                    else:
                        cate_duplicated_dict[k] = v
            
            duplicated_dict[cate] = cate_duplicated_dict
            os.remove(os.path.join(deduplicated_ids_dir, "duplicated_ids.json"))
            write_json(os.path.join(deduplicated_ids_dir, "duplicated_ids.json"), duplicated_dict) 
            print("write duplicated ids finished!")
        print("update minhashes length: ", len(list(minhashes.keys())))
        return minhashes
    elif mode == "multi_search":
        start = time.time()
        filenames = os.listdir(os.path.join(hashindex_dir, cate))
        is_read_file, new_minhashes, duplicated_ids = [], {}, []
        search_minhashes = multi_get_minhash(id_to_text_dict, processing, workers)
        while len(is_read_file) != len(filenames):
            minhashes, is_read_file = read_part_minhashes_index(hashindex_dir, cate, is_read_file)
            part_minihashes, part_duplicated_ids = multi_hash_search(search_minhashes, minhashes)
            # part_minihashes, part_duplicated_ids = hash_search(id_to_text_dict, minhashes, processing)
            new_minhashes = dict_update_intersection(new_minhashes,part_minihashes)
            duplicated_ids.append(part_duplicated_ids)
        # 保存minhashes
        # print(len(duplicated_ids))
        end = time.time()
        print("multi_search speed ", end-start)
        # return 
        write_part_minhashes_index(hashindex_dir, cate, name, new_minhashes)
        print("deduplicated minhashes finished!")
        # 保存去重后的ids
        all_deduplicated_ids_dict = read_json(os.path.join(deduplicated_ids_dir, "deduplicated_ids.json"))
        all_deduplicated_ids_dict[cate].extend(list(new_minhashes.keys()))
        os.remove(os.path.join(deduplicated_ids_dir, "deduplicated_ids.json"))
        write_json(os.path.join(deduplicated_ids_dir, "deduplicated_ids.json"), all_deduplicated_ids_dict)
        print("write deduplicated ids finished!")
        # 保存重复的ids
        duplicated_res = {}
        for values in duplicated_ids:
            for value in values:
                for k,v in value.items():
                    if k in duplicated_res.keys():
                        duplicated_res[k].extend(v)
                    else:
                        duplicated_res[k] = v
                        
        if os.path.exists(os.path.join(deduplicated_ids_dir, "duplicated_ids.json")):
            duplicated_dict = read_json(os.path.join(deduplicated_ids_dir, "duplicated_ids.json"))
            cate_duplicated_dict = duplicated_dict[cate]
            for key in duplicated_res.keys():
                if key in cate_duplicated_dict.keys():
                    cate_duplicated_dict[key].extend(duplicated_res[key])
                else:
                    cate_duplicated_dict[key] = duplicated_res[key]
            duplicated_dict[cate] = cate_duplicated_dict
            os.remove(os.path.join(deduplicated_ids_dir, "duplicated_ids.json"))
            write_json(os.path.join(deduplicated_ids_dir, "duplicated_ids.json"), duplicated_dict)  
            print("write duplicated ids finished!")      
        return new_minhashes
    
    


if __name__=="__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data_path", type=str, default="D:\\github_lab\\Lumix\\Data\\store\\webs\\trade_webs_cleaned_8_unique.json", help="data path")
    parser.add_argument("--deduplicate", type=str, choices=["minhash", "simhash"], default="minhash", help="deduplicate method")
    parser.add_argument("--processing", type=str, choices=["single", "multi"], default="multi", help="processing method")
    parser.add_argument("--workers", type=int, default=4, help="workers")
    parser.add_argument("--deduplicate_mode", type=str, choices=["store", "search", "multi_search"], default="store", help="deduplicate mode")
    parser.add_argument("--multi_deduplicate", type=bool, default=False, help="use multi deduplicate")
    parser.add_argument("--deduplicated_ids_dir", type=str, default="D:\\github_lab\\Lumix\\Data\\deduplicated", help="index path")
    parser.add_argument("--hashindex_dir", type=str, default="D:\\github_lab\\Lumix\\Data\\hash_index", help="index path")
    parser.add_argument("--update_minhashes", type=bool, default=True, help="update minhashes")
    parser.add_argument("--use_hash", type=bool, default=True, help="use hash")
    parser.add_argument("--name", type=str, default="langchao", help="name")
    args = parser.parse_args()
    
    main(args)
    
    