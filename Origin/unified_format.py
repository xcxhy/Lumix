import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import argparse
from tqdm import tqdm
from utils import *
from itertools import chain, repeat
from concurrent import futures

def unified(path, id_path, save_dir):
    # 判断路径是文件还是路径
    if os.path.isfile(path):
        files_path = [path]
    elif os.path.isdir(path):
        files = os.listdir(path)
        files_path = [os.path.join(path, file) for file in files if "error" in file]
    
    for one_path in tqdm(files_path):
        # 读取ID文件
        if os.path.exists(id_path):
            files_ids = set(read_json(id_path)[0]["ids"])
        else:
            files_ids = set([])
        print("files_ids: ", len(files_ids))
        # 解析文件名
        basename = os.path.basename(one_path)
        if "trade" in basename:
            if "networks" in basename:
                new_files_ids = read_trade_networks(one_path, files_ids, save_dir)
            elif "wiki" in basename:
                new_files_ids = read_trade_wiki(one_path, files_ids, save_dir)
            elif "forum" in basename:
                new_files_ids = read_trade_forum(one_path, files_ids, save_dir)
            elif "webs" in basename:
                new_files_ids = read_trade_webs(one_path, files_ids, save_dir)
            elif "c4" in basename:
                new_files_ids = read_trade_C4(one_path, files_ids, save_dir)
            elif "langchao" in basename and "error" not in basename:
                new_files_ids = read_trade_langchao(one_path, files_ids, save_dir)
            elif "langchao" in basename and "error" in basename:
                new_files_ids = read_trade_langchao_error(one_path, files_ids, save_dir)
            elif "wudao" in basename:
                new_files_ids = read_trade_wudao(one_path, files_ids, save_dir)
            elif "books" in basename:
                new_files_ids = read_trade_books(one_path, files_ids, save_dir)
            else:
                new_files_ids = read_trade_customs(one_path, files_ids, save_dir)
        else:
            pass
        # 存储ID文件
        print("new_files_ids: ", len(new_files_ids))
        # 删除原ID文件
        if os.path.exists(id_path):
            os.remove(id_path)
        write_json(id_path, [{"ids": list(new_files_ids)}])
                
    return 
    

def read_trade_networks(path, files_ids, save_dir):
    # 解析名称
    basename = os.path.basename(path)
    if not os.path.exists(os.path.join(save_dir, "networks")):
        os.makedirs(os.path.join(save_dir, "networks"))
    else:
        if os.path.exists(os.path.join(save_dir, "networks", basename.replace(".json", "_unique.json"))):
            return files_ids
    data = read_json(path)
    for value in tqdm(data):
        while True:
            id = get_file_id()
            if id in files_ids:
                id = get_file_id()
            else:
                break
        files_ids.add(id)
        value["unique_id"] = id
    
    write_json(os.path.join(save_dir, "networks", basename.replace(".json", "_unique.json")), data)
    return files_ids

def read_trade_wiki(path, files_ids, save_dir):
    # 解析名称
    basename = os.path.basename(path)
    if not os.path.exists(os.path.join(save_dir, "wiki")):
        os.makedirs(os.path.join(save_dir, "wiki"))
    else:
        if os.path.exists(os.path.join(save_dir, "wiki", basename.replace(".json", "_unique.json"))):
            return files_ids
    data = read_json(path)
    for value in tqdm(data):
        while True:
            id = get_file_id()
            if id in files_ids:
                id = get_file_id()
            else:
                break
        files_ids.add(id)
        value["unique_id"] = id
    
    write_json(os.path.join(save_dir, "wiki", basename.replace(".json", "_unique.json")), data)
    return files_ids

def read_trade_forum(path, files_ids, save_dir):
    # 解析名称
    basename = os.path.basename(path)
    if not os.path.exists(os.path.join(save_dir, "forum")):
        os.makedirs(os.path.join(save_dir, "forum"))
    else:
        if os.path.exists(os.path.join(save_dir, "forum", basename.replace(".json", "_unique.json"))):
            return files_ids
    data = read_json(path)
    new_data = []
    for value in tqdm(data):
        new_dict = {}
        while True:
            id = get_file_id()
            if id in files_ids:
                id = get_file_id()
            else:
                break
        files_ids.add(id)
        new_dict["unique_id"] = id
        new_dict["text"] = value["clean_answer"]
        new_dict["meta"] = value
        new_data.append(new_dict)
    write_json(os.path.join(save_dir, "forum", basename.replace(".json", "_unique.json")), new_data)
    return files_ids

def read_trade_webs(path, files_ids, save_dir):
    # 解析名称
    basename = os.path.basename(path)
    if not os.path.exists(os.path.join(save_dir, "webs")):
        os.makedirs(os.path.join(save_dir, "webs"))
    else:
        if os.path.exists(os.path.join(save_dir, "webs", basename.replace(".json", "_unique.json"))):
            return files_ids
    data = read_json(path)
    for value in tqdm(data):
        while True:
            id = get_file_id()
            if id in files_ids:
                id = get_file_id()
            else:
                break
        files_ids.add(id)
        value["unique_id"] = id
    
    write_json(os.path.join(save_dir, "webs", basename.replace(".json", "_unique.json")), data)
    return files_ids

def read_trade_C4(path, files_ids, save_dir):
    # 解析名称
    basename = os.path.basename(path)
    if not os.path.exists(os.path.join(save_dir, "c4")):
        os.makedirs(os.path.join(save_dir, "c4"))
    else:
        if os.path.exists(os.path.join(save_dir, "c4", basename.replace(".json", "_unique.json"))):
            return files_ids
    data = read_json(path)
    new_data = []
    for value in tqdm(data):
        new_dict = {}
        while True:
            id = get_file_id()
            if id in files_ids:
                id = get_file_id()
            else:
                break
        files_ids.add(id)
        new_dict["unique_id"] = id
        new_dict["text"] = value["text"]
        new_dict["meta"] = value["meta"]
        new_dict["meta"]["match-keyword"] = value["match-keyword"]
        new_data.append(new_dict)
    
    write_json(os.path.join(save_dir, "c4", basename.replace(".json", "_unique.json")), new_data)
    
    return files_ids

def read_trade_langchao(path, files_ids, save_dir):
    # 解析名称
    basename = os.path.basename(path)
    if not os.path.exists(os.path.join(save_dir, "langchao")):
        os.makedirs(os.path.join(save_dir, "langchao"))
    else:
        if os.path.exists(os.path.join(save_dir, "langchao", basename.replace(".json", "_unique.json"))):
            return files_ids
    data = read_json(path)
    new_data = []
    for value in tqdm(data):
        new_dict = {}
        while True:
            id = get_file_id()
            if id in files_ids:
                id = get_file_id()
            else:
                break
        files_ids.add(id)
        new_dict["unique_id"] = id
        new_dict["text"] = value["text"]
        new_dict["meta"] = {"match-keyword": value["match-keyword"]}
        new_data.append(new_dict)
    
    write_json(os.path.join(save_dir, "langchao", basename.replace(".json", "_unique.json")), new_data)
    
    return files_ids

def read_trade_langchao_error(path, files_ids, save_dir):
    # 解析名称
    basename = os.path.basename(path)
    if not os.path.exists(os.path.join(save_dir, "langchao_error")):
        os.makedirs(os.path.join(save_dir, "langchao_error"))
    else:
        if os.path.exists(os.path.join(save_dir, "langchao_error", basename.replace(".json", "_unique.json"))):
            return files_ids
    data = read_json(path)
    new_data = []
    for value in tqdm(data):
        new_dict = {}
        while True:
            id = get_file_id()
            if id in files_ids:
                id = get_file_id()
            else:
                break
        files_ids.add(id)
        new_dict["unique_id"] = id
        new_dict["text"] = value["text"]
        new_dict["meta"] = {"match-keyword": value["match-keyword"]}
        new_data.append(new_dict)
    
    write_json(os.path.join(save_dir, "langchao_error", basename.replace(".json", "_unique.json")), new_data)
    
    return files_ids

def read_trade_wudao(path, files_ids, save_dir):
    # 解析名称
    basename = os.path.basename(path)
    if not os.path.exists(os.path.join(save_dir, "wudao")):
        os.makedirs(os.path.join(save_dir, "wudao"))
    else:
        if os.path.exists(os.path.join(save_dir, "wudao", basename.replace(".json", "_unique.json"))):
            return files_ids
    data = read_json(path)
    new_data = []
    for value in tqdm(data):
        new_dict = {}
        while True:
            id = get_file_id()
            if id in files_ids:
                id = get_file_id()
            else:
                break
        files_ids.add(id)
        new_dict["unique_id"] = id
        new_dict["text"] = value["text"]
        new_dict["meta"] = value["meta"]
        new_dict["meta"]["match-keyword"] = value["match-keyword"]
        new_data.append(new_dict)

    write_json(os.path.join(save_dir, "wudao", basename.replace(".json", "_unique.json")), new_data)
    
    return files_ids

def read_trade_customs(path, files_ids, save_dir):
    
    return files_ids



def read_trade_books(path, files_ids, save_dir):
    # 解析名称
    basename = os.path.basename(path)
    data = read_json(path)
    new_data = []
    for value in tqdm(data):
        new_dict = {}
        while True:
            id = get_file_id()
            if id in files_ids:
                id = get_file_id()
            else:
                break
        files_ids.add(id)
        new_dict["unique_id"] = id
        new_dict["text"] = value["text"]
        meta = value.pop("text")
        new_dict["meta"] = meta
        new_data.append(new_dict)
    
    if not os.path.exists(os.path.join(save_dir, "books")):
        os.makedirs(os.path.join(save_dir, "books"))
    write_json(os.path.join(save_dir, "books", basename.replace(".json", "_unique.json")), new_data)
    return files_ids



if __name__=="__main__":
    path = "/hdfs_nfs_mount/LLM_data/llm_basedata/process/origin/trade_networks_2023_08_23.json"
    id_path = "/hdfs_nfs_mount/LLM_data/llm_basedata/process/unique_ids.json"
    save_dir = "/hdfs_nfs_mount/LLM_data/llm_basedata/process/unique"
    
    
    networks_path = "/hdfs_nfs_mount/LLM_data/llm_basedata/process/origin/trade_networks_2023_08_23.json"
    wiki_path = "/hdfs_nfs_mount/LLM_data/llm_basedata/process/origin/trade_wiki_2023.json"
    forum_fob_path = "/hdfs_nfs_mount/LLM_data/llm_basedata/process/origin/trade_forum_fob_2023_09_08.json"
    forum_go_path = "/hdfs_nfs_mount/LLM_data/llm_basedata/process/origin/trade_forum_globalimporter_2023_09_08.json"
    webs_path = "/hdfs_nfs_mount/LLM_data/llm_basedata/process/origin/trade_webs_2023.json"
    c4_path = "/hdfs_nfs_mount/LLM_data/llm_basedata/process/origin/trade_c4_choosed.json"
    langchao_path = "/hdfs_nfs_mount/LLM_data/llm_basedata/langchao/choosed"
    wudao_path = "/hdfs_nfs_mount/LLM_data/llm_basedata/wudao/relevant"
    books_path = "/hdfs_nfs_mount/LLM_data/llm_basedata/process/origin/trade_books_2023.json"
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--path", type=str, default="D:\\github_lab\\Lumix\\Data\\store\\trade_webs_cleaned_8.json", help="data_path")
    parser.add_argument("--id_path", type=str, default="D:\\github_lab\\Lumix\\Data\\unique_ids.json", help="id path")
    parser.add_argument("--save_dir", type=str, default="D:\\github_lab\\Lumix\\Data\\unique\\trade", help="save dir")
    args = parser.parse_args()
    
    unified(args.path, args.id_path, args.save_dir)
    