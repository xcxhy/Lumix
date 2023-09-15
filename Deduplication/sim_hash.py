import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import numpy as np 
from utils import *
import re
from simhash import Simhash, SimhashIndex
from tqdm import tqdm
from itertools import chain

# view simhash value
def get_features(s):
    width = 3
    s = s.lower()
    s = re.sub(r'[^\w]+', '', s)
    result = [s[i:i+width] for i in range(max(len(s) - width + 1, 1))]
    return result

def get_simhash_index(data):
    objs = [(str(k), Simhash(get_features(v))) for k, v in tqdm(data.items())]
    index = SimhashIndex(objs, k=3)
    result, nums = [], 0
    for key in tqdm(list(data.keys())):
        s1 = Simhash(get_features(data[key]))
        res_list = index.get_near_dups(s1)
        if len(res_list) == 1 and res_list[0] == key:
            continue
        else:
            nums += 1
            result.append({"key": key, "value": [int(vl) for vl in res_list]})
    return result

def sim_hash_main(text_list):
    text_dict = {}
    i = 0
    for value in tqdm(text_list):
        text_dict[str(i)] = value
        i+=1
    result = get_simhash_index(text_dict)
    merge_list = []
    for value in result:
        merge_list.append(value["value"])
    merged_list = merge_list_with_intersection(merge_list)
    
    text_dict = {}
    for merged in merged_list:
        texts = []
        for index in merged:
            texts.append(text_list[int(index)])
        text_dict[str(merged)] = texts
    return merged_list, text_dict
    
def merge_list_with_intersection(lists):
    merged = []
    for sublist in lists:
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

    
        
        
    
