'''
Author: your name
Date: 2023-09-15 14:32:30
LastEditTime: 2023-09-18 20:31:22
LastEditors: xuhao0101
Description: In User Settings Edit
FilePath: \Lumix\clean_main.py
'''
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import argparse
from tqdm import tqdm
from utils import *
from itertools import chain, repeat
from concurrent import futures
from Clean.clean_content import *
from Clean.clean import *

def deduplication(data_path, mode, hash_path, duplication):
    # determine is dir or file
    if os.path.isfile(data_path):
        data = read_json(data_path)
    else:
        file_list = os.listdir(data_path)
        for file in file_list:
            # 以.json结尾的文件
            if not file.endswith('.json'):
                print(r"data path is not a real path, you must input a dir with json files or a json file")
                return 
        if mode:
            data = read_json(os.path.join(data_path, file_list[0]))
            data = data[:1000]
        else:
            data = []
            for name in file_list:
                d = read_json(os.path.join(data_path, name))
                data.extend(d)
    
    # create id dict for deduplication
    text_to_value_dict, index_to_text_dict = {}, {}
    for i in tqdm(range(len(data))):
        value = data[i]
        text_to_value_dict[value["text"]] = value
        index_to_text_dict[i] = value["text"]
        
    # set deduplication
    set_dict = set_deduplicated(index_to_text_dict)
    
    # hash deduplication
    if os.path.isdir(duplication):
        sim_hash_path = os.path.join(duplication, 'sim.json')
        min_hash_path = os.path.join(duplication, 'min.json')
        duplication_path = os.path.join(duplication, 'duplication.json')
    else:
        print(r"duplication path is not a real path, you must input a dir")
        return
        
    hash_dict = hash_deduplicated(set_dict, sim_hash_path, min_hash_path, duplication_path)
    if os.path.isdir(hash_path):
        save_list = get_save_list(text_to_value_dict, hash_dict)
        if len(save_list) > 1000000:
            save_parts_list = [save_list[i:i+1000000] for i in range(0, len(save_list), 1000000)]
            for i in range(len(save_parts_list)):
                write_json(save_parts_list[i], os.path.join(hash_path, 'hash_{}.json'.format(i)))
        else:
            write_json(save_list, os.path.join(hash_path, 'hash.json'))
    return 

def clean(data_path, save_path, use_customed=False):
    # determine is dir or file
    if os.path.isfile(data_path):
        data = read_json(data_path)
    else:
        file_list = os.listdir(data_path)
        for file in file_list:
            # 以.json结尾的文件
            if not file.endswith('.json'):
                print(r"data path is not a real path, you must input a dir with json files or a json file")
                return 
        data = []
        for name in file_list:
            d = read_json(os.path.join(data_path, name))
            data.extend(d)
    
    # start clean data
    print("start clean data!")
    new_data = []
    for value in tqdm(data):
        text = value["text"]
        if use_customed:
            new_text = clean_text_customed(text)
        else:
            new_text = clean_text(text)
        # delete text with length less than 50
        if len(new_text) < 50:
            continue
        else:
            value["text"] = new_text
            new_data.append(value)
    print("clean data finished!")
    if os.path.isdir(save_path):
        if len(new_data) > 1000000:
            save_parts_list = [new_data[i:i+1000000] for i in range(0, len(new_data), 1000000)]
            for i in range(len(save_parts_list)):
                write_json(save_parts_list[i], os.path.join(save_path, 'clean_{}.json'.format(i)))
        else:
            write_json(new_data, os.path.join(save_path, 'clean.json'))
    return

def classify(data_path, save_path, model_path):
    # read data
    if os.path.isfile(data_path):
        data = read_json(data_path)
    else:
        file_list = os.listdir(data_path)
        for file in file_list:
            # 以.json结尾的文件
            if not file.endswith('.json'):
                print(r"data path is not a real path, you must input a dir with json files or a json file")
                return 
        data = []
        for name in file_list:
            d = read_json(os.path.join(data_path, name))
            data.extend(d)
            
    id_to_value_dict, id_to_text_dict = {}, {}
    for i in tqdm(range(len(data))):
        value = data[i]
        id_to_value_dict[str(i)] = value
        id_to_text_dict[str(i)] = value["text"]
        
    # classify 
    print("start classify data!")
    new_id_to_text_dict = fasttext_content_inference(id_to_text_dict, model_path)
    new_keys = list(new_id_to_text_dict.keys())
    print("classify data finished!")
    
    new_data, error_data = [], []
    for key in tqdm(list(id_to_value_dict.keys())):
        value = id_to_value_dict[key]
        if key in new_keys:
            new_data.append(value)
        else:
            error_data.append(value)
    print("normal data length: ", len(new_data))
    print("error data length: ", len(error_data))
    if os.path.isdir(save_path):
        if len(new_data) > 1000000:
            save_parts_list = [new_data[i:i+1000000] for i in range(0, len(new_data), 1000000)]
            for i in range(len(save_parts_list)):
                write_json(save_parts_list[i], os.path.join(save_path, 'classify_{}.json'.format(i)))
        else:
            write_json(new_data, os.path.join(save_path, 'classify.json'))
        if len(error_data) > 1000000:
            save_parts_list = [error_data[i:i+1000000] for i in range(0, len(error_data), 1000000)]
            for i in range(len(save_parts_list)):
                write_json(save_parts_list[i], os.path.join(save_path, 'classify_error_{}.json'.format(i)))
        else:
            write_json(error_data, os.path.join(save_path, 'classify_error.json'))
    return


if __name__=="__main__":
    # init args
    parser = argparse.ArgumentParser()
    parser.add_argument('--data_path', type=str, default='data', help='path to dataset')
    parser.add_argument('--save_path', type=str, default='data', help='path to save dataset')
    parser.add_argument('--deduplicate', type=bool, default=True, help='whether to deduplicate')
    parser.add_argument('--clean', type=bool, default=True, help='whether to clean')
    parser.add_argument('--use_clean_customed', type=bool, default=False, help='whether to use clean customed')
    parser.add_argument('--classify', type=bool, default=True, help='whether to classify')
    parser.add_argument('--duplication_path', type=str, default='duplication', help='path to save duplication')
    parser.add_argument('--classify_model_path', type=str, default='classify_model', help='path to save classify model')
    parser.add_argument('--few_sample', type=bool, default=True, help='whether to use few sample')
    
    args = parser.parse_args()
    
    hash_path = os.path.join(args.save_path, "deduplicaton")
    # set_path = os.path.join(args.save_path, "deduplicaton")
    # sim_path = os.path.join(args.duplication_path, 'sim.json')
    # min_path = os.path.join(args.duplication_path, 'min.json')
    # duplication_path = os.path.join(args.duplication_path, 'duplication.json')
    clean_path = os.path.join(args.save_path, "clean",)
    classify_path = os.path.join(args.save_path, "classification")
    
    if args.deduplicate:
        if not os.path.exists(hash_path):
            os.makedirs(hash_path)
        deduplication(args.data_path, args.few_sample, hash_path, args.duplication_path)
    if args.clean:
        if not os.path.exists(clean_path):
            os.makedirs(clean_path)
        clean(hash_path, clean_path, args.use_clean_customed)
    if args.classify:
        if not os.path.exists(classify_path):
            os.makedirs(classify_path)
        classify(clean_path, classify_path, args.classify_model_path)