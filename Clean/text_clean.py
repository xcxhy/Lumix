import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import *
from clean_utils import *
import argparse
from itertools import chain, repeat
from multiprocessing import Pool
from concurrent import futures

def main_clean(args):
    files = os.listdir(args.data_path)
    data_paths = [os.path.join(args.data_path, file) for file in files]
    for data_path in tqdm(data_paths):
        print("start clean: ", data_path)
        data = read_ids(data_path, args.part_ids_path, args.ids_path)
        # data = data[:2000]
        start = time.time()
        if args.workers == 1:
            new_list = []
            for value in tqdm(data):
                text = value["text"]
                value["text"] = sentence_clean(text)
                new_list.append(value)
        else:
            new_list = multi_worker_clean(data, args.workers)
        end = time.time()
        print("all time: ", end-start)
        if "/" in data_path:
            file_name_list = data_path.split("/")
        elif "\\" in data_path:
            file_name_list = data_path.split("\\")
        cate = file_name_list[-2]
        filename = file_name_list[-1]
        
        if not os.path.exists(os.path.join(args.output_file,cate)):
            os.makedirs(os.path.join(args.output_file,cate))
        
        write_json(os.path.join(args.output_file, cate, filename), new_list)
        print("write file: ", os.path.join(args.output_file, cate, filename))
    return 


if __name__=="__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data_path", type=str, default="Data\\classify\\webs")
    parser.add_argument("--part_ids_path", type=str, default="Data\\trade_filename_to_ids.json")
    parser.add_argument("--ids_path", type=str, default="Data\\ids\\classificated\\filtered_ids.json")
    parser.add_argument("--output_file", type=str, default="Data\\clean")
    parser.add_argument("--workers", type=int, default=2)
    args = parser.parse_args()
    main_clean(args)