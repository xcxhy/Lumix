import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from Filter.filter_utils import *
from utils import *
from clean_content import *
from clean import *

# 根据part_ids和all_ids，过滤出需要的数据
def read_ids(data_path, part_ids_path, ids_path):
    # speacial case
    if "classify" not in data_path:
        real_data_path = data_path.replace("unique", "classify")
    else:
        real_data_path = data_path
        data_path = data_path.replace("classify", "unique")
    data, cate = read_file_or_dir(real_data_path)
    # read part ids
    
    if os.path.isfile(real_data_path):
        part_ids = read_json(part_ids_path)[data_path]
    
    ids = read_json(ids_path)[cate]
    
    set_part_ids = set(part_ids)
    set_ids = set(ids)
    
    filter_ids = set_part_ids & set_ids
    print("filter_ids: ", len(filter_ids))
    
    # filter data
    filter_data = [value for value in tqdm(data) if value["unique_id"] in filter_ids]
    
    return filter_data

def document_clean():
    
    
    return 

def sentence_clean(text):
    # 删除连续得符号
    text = remove_consecutive_symbols(text)
    # 删除特殊符号
    text = remove_special_unicode2(text)
    # 分割粘连英文
    text = replace_wordninja(text)
    # 删除【】内得内容
    # text = re.sub(r'【.*?】', '', text)
    return text

def sentence_clean_single(data):
    new_data = []
    for text in data:
        value = sentence_clean(text)
        new_data.append(value)
    # for value in data:
    #     text = value["text"]
    #     value["text"] = sentence_clean(text)
    #     new_data.append(value)
    return new_data

def multi_worker_clean(data, workers):
    text_list = [value["text"] for value in data]
    pool = Pool(processes=workers)
    chunk_size = len(text_list)//pool._processes
    res = pool.map(sentence_clean, text_list, chunksize=chunk_size)
    res = list(res)
    pool.close()
    pool.join()
    # with Pool(processes=workers) as pool:
    #     res = pool.starmap(sentence_clean, text_list)
    #     res = list(res)
    # with futures.ThreadPoolExecutor(workers) as executor:
    #     res = executor.map(sentence_clean, text_list)
    #     res = list(res)
    new_list = []  
    for i in range(len(data)):
        value = data[i]
        value["text"] = res[i]
        new_list.append(value)
        
    # text_list = [value["text"] for value in data]
    # with Pool(processes=workers) as pool:
    #     result = pool.starmap(sentence_clean_single, text_list)
    return new_list
    