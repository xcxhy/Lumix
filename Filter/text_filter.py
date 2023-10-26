import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import *
from filter_utils import *
import argparse
from itertools import chain, repeat
from multiprocessing import Pool
from concurrent import futures
# import fasttext

def main(args):
    # read data and category
    data, cate = read_file_or_dir(args.data_path)
    # read part ids
    if os.path.isfile(args.data_path):
        part_ids = read_json(args.part_ids_path)[args.data_path]
    elif os.path.isdir(args.data_path):
        part_ids = []
        files = os.listdir(args.data_path)
        for file in tqdm(files):
            if file.endswith(".json") or file.endswith(".jsonl"):
                file_path = os.path.join(args.data_path, file)
                part_ids.extend(read_json(args.part_ids_path)[file_path])
    # read all ids
    ids = read_json(args.ids_path)[cate]
    
    # Filter the data to be filtered(intersection of part_ids and ids)
    set_part_ids = set(part_ids)
    set_ids = set(ids)
    
    filter_ids = set_part_ids & set_ids
    print("filter_ids: ", len(filter_ids))
    
    # filter data
    filter_before_data = [value for value in tqdm(data) if value["unique_id"] in filter_ids]
    
    # filter short text
    long_data, short_data = filter_short_text(filter_before_data, args.tokenizer_path, args.workers)
    
    # filter adivertisement text
    # filter_ad_data = filter_advertisement_text(filter_before_data[:1000], args.workers)
    
    # filter messy code text
    # filter_messy_data = filter_messy_code_text(long_data)
    
    # filter semantic text
    # filter_semantic_data = filter_semantic_text(filter_before_data)
    
    # final data
    final_data = long_data
    filtered_ids = [value["unique_id"] for value in final_data]
    print("final_data: ", len(final_data))
    
    # judge output file
    if not os.path.exists(args.output_file):
        os.makedirs(args.output_file)
    if not os.path.exists(os.path.join(args.output_file, "filtered_ids.json")):
        write_json(os.path.join(args.output_file, "filtered_ids.json"), {cate:filtered_ids})
    else:
        key_dict = read_json(os.path.join(args.output_file, "filtered_ids.json"))
        keys = list(key_dict.keys())
        if cate in keys:
            old_list = key_dict[cate]
            old_list.extend(filtered_ids)
            key_dict[cate] = list(set(old_list))
        else:
            key_dict[cate] = filtered_ids
        # delete old file
        os.remove(os.path.join(args.output_file, "filtered_ids.json"))
        write_json(os.path.join(args.output_file, "filtered_ids.json"), key_dict)
        print(len(key_dict[cate]))
    return 



if __name__=="__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data_path", type=str, default="Data\\unique\\trade\\webs", help="input file")
    parser.add_argument("--part_ids_path", type=str, default="Data\\trade_filename_to_ids.json", help="part ids file")
    parser.add_argument("--ids_path", type=str, default="Data\\ids\\deduplicated\\deduplicated_ids.json", help="all ids file")
    parser.add_argument("--output_file", type=str, default="Data\\ids\\classificated", help="output file")
    parser.add_argument("--tokenizer_path", type=str, default="Model\\llama2_tokenizer\\llama2-7b-hf")
    parser.add_argument("--workers", type=int, default=1, help="workers")
    args = parser.parse_args()
    main(args)
    

