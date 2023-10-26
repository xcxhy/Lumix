
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import argparse
from tqdm import tqdm
from utils import *

def confirm(data_dir, save_path, files_path):
    # 判断是否有文件夹
    if not os.path.exists(data_dir):
        print("dir not exists")
        return
    else:
        ids,file_ids, nums = [], {}, 0
        cates = os.listdir(data_dir)
        for cate in cates:
            cate_path = os.path.join(data_dir, cate)
            filecates = os.listdir(cate_path) 
            for filename in filecates:
                data = read_json(os.path.join(cate_path, filename))
                data_keys = [value['unique_id'] for value in data]
                nums += len(data_keys)
                ids.extend(data_keys)
                file_ids[str(os.path.join(cate_path, filename))] = data_keys
                    
                        
        print("total nums: ", nums)
        print("total ids: ", len(set(ids)))
        if os.path.exists(save_path):
            os.remove(save_path)
        write_json(save_path, [{"ids": list(set(ids))}])
        if os.path.exists(files_path):
            os.remove(files_path)
        write_json(files_path, file_ids)
        
                        
    
    
    return 

if __name__=="__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--dir', type=str, default='Data\\unique')
    parser.add_argument('--save_path', type=str, default='Data\\unique_ids.json')
    parser.add_argument("--files_path", type=str, default="Data\\trade_filename_to_ids.json")
    args = parser.parse_args()
    
    confirm(args.dir, args.save_path, args.files_path)
    