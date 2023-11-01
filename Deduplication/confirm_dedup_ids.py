import os
import sys
sys.path.append("/home/xuhao/xcxhy/Focus_Dataset_v2")
sys.path.append("/home/xuhao/xcxhy")
import argparse
from tqdm import tqdm
from Focus_Dataset_v2.utils import *

def confirm_deduplicated(origin_ids_path, all_ids_path, files_dir, cate):
    
    # judge the origin_ids_path is exists
    if os.path.exists(origin_ids_path):
        origin_ids = read_json(origin_ids_path)[cate]
        print("origin_ids: ", len(origin_ids))
    else:
        print("origin_ids_path not exists")
    
    # read all ids
    all_ids = read_json(all_ids_path)[0]["ids"]
    print("all_ids: ", len(all_ids))
    deduplicated_ids = read_deduplicated_ids(files_dir, cate)
    # judge the intersection of origin_ids and all_ids
    set_origin_ids = set(origin_ids)
    set_all_ids = set(all_ids)
    set_dedup_ids = set(deduplicated_ids)
    
    if len(set_dedup_ids) == len(set_origin_ids):
        print("deduplicated_ids is right")
        section_ids = set_origin_ids & set_all_ids
        if len(section_ids) == len(set_origin_ids):
            print("section_ids is right")
        else:
            # os.remove(origin_ids_path)
            # write_json(origin_ids_path, {cate: deduplicated_ids})
            print("section_ids is wrong")
    else:
        # os.remove(origin_ids_path)
        # write_json(origin_ids_path, {cate: deduplicated_ids})
        print("deduplicated_ids is wrong")
    
    mode = int(input("do you need update the deduplicated ids file? (1/0)"))
    if mode:
        os.remove(origin_ids_path)
        write_json(origin_ids_path, {cate: deduplicated_ids})
    else:
        print("exit!")
    
    # read files ids
    return 
    
    
    

def read_deduplicated_ids(files_dir, cate):
    if os.path.exists(os.path.join(files_dir, cate)):
        filenames = os.listdir(os.path.join(files_dir, cate))
        ids_list = []
        for filename in tqdm(filenames):
            hashes_data = read_minhashes(os.path.join(files_dir, cate, filename))
            hashes_list = list(hashes_data.keys())
            print("hashes_list: ", len(hashes_list))
            ids_list.extend(hashes_list)
        print("ids_list: ", len(ids_list))
    return ids_list


if __name__=="__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--origin_ids_path', type=str, default='/hdfs_nfs_mount/LLM_data/llm_basedata/process/ids/deduplicated/deduplicated_ids.json')
    parser.add_argument('--all_ids_path', type=str, default='/hdfs_nfs_mount/LLM_data/llm_basedata/process/unique_ids.json')
    parser.add_argument('--files_dir', type=str, default='/hdfs_nfs_mount/LLM_data/llm_basedata/process/hash_index')
    parser.add_argument('--cate', type=str, default='trade')
    args = parser.parse_args()
    
    confirm_deduplicated(args.origin_ids_path, args.all_ids_path, args.files_dir, args.cate)