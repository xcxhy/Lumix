import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import json
import argparse
import re
import enchant
from langdetect import detect
from tqdm import tqdm
from utils import *
from itertools import chain, repeat
from concurrent import futures
from Deduplication.levenshtein_deduplication import fuzzy_deduplicate
# from Deduplication.minhash_lsh import min_hashlsh_main
# from Deduplication.sim_hash import sim_hash_main
from clean_content import *
import emoji
from autocorrect import Speller

# input id-text dict, output id-text dict 
def set_deduplicated(data_dict):
    text_list = [data_dict[key] for key in tqdm(list(data_dict.keys()))]
    print("origin sample number: ", len(text_list))
    text_to_index_dict = dict(zip(text_list, list(range(len(text_list))))) # text-index dict, convenient for later lookup
    # set deduplication
    set_list = list(set(text_list))
    if "" in set_list:
        set_list.remove("")
    if " " in set_list:
        set_list.remove(" ")
    print("set deduplicated sample number: ", len(set_list))
    set_dict = {}
    for set_text in tqdm(set_list):
        set_dict[text_to_index_dict[set_text]] = set_text
    return set_dict


# input id-text dict to deduplicate by hash (include fuzzy deduplication)
def hash_deduplicated(data_dict, sim_hash_path=None, min_hash_path=None, duplication_path=None, use_fuzzy=False):
    keys = list(data_dict.keys())
    text_list = [data_dict[key] for key in tqdm(keys)]
    print("origin sample number: ", len(text_list))
    
    # reverse dict key-value
    text_to_index_dict = dict(zip(text_list, keys)) # text-index dict, convenient for later lookup
    # split data by length
    text_up_dict, text_up_list, = {}, []
    text_down_dict, text_down_list = {}, []
    print("Start split data!")
    for j in tqdm(range(len(text_list))):
        text = text_list[j]
        tokenizer_length = len(text)
        if tokenizer_length > 500:
            text_up_dict[text_to_index_dict[text]] = text
            text_up_list.append(text)
        else:
            text_down_dict[text_to_index_dict[text]] = text
            text_down_list.append(text)
    print("End split data!")
    print("over threshold number: ", len(text_up_list))
    print("below threshold number: ", len(text_down_list))
    # minhash_lsh deduplication
    min_merged_list, min_text_dict = min_hashlsh_main(text_down_list)
    min_merged_list = get_hash_index(min_merged_list, text_down_list, text_to_index_dict)
    if not min_hash_path:
        write_json(min_hash_path, [min_text_dict]) # save min hash result
        print("write minhash result!")
    # simhash deduplication
    sim_merged_list, sim_text_dict = sim_hash_main(text_up_list)
    sim_merged_list = get_hash_index(sim_merged_list, text_up_list, text_to_index_dict)
    if not sim_hash_path:
        write_json(sim_hash_path, [sim_text_dict]) # save sim hash result
        print("write simhash result!")
    # concat minhash_lsh and simhash result
    merged_list = sim_merged_list + min_merged_list
    merged_list = merge_list_with_intersection(merged_list)
    print("concated repeated group number: ", len(merged_list))
    # flatten the repeated list
    merged_one_dimension_list = list(chain.from_iterable(merged_list))
    print("concated repeated number: ", len(merged_one_dimension_list))
    
    # dedpulicated dict with index-text
    clean_dict = {}
    for key in tqdm(list(data_dict.keys())):
        if key in merged_one_dimension_list:
            continue
        else:
            clean_dict[key] = data_dict[key]
      
    # use fuzzy deduplication(not open)
    if use_fuzzy:
        duplicated_list = []
        for value in merged_list:
            dup_list = []
            for idx in value:
                dup_list.append(data_dict[idx])
            duplicated_list.append(dup_list)
        fuzzy_merged_list = fuzzy_deduplicated(duplicated_list)
        duzzy_merged_dict = {}
        for text in fuzzy_merged_list:
            duzzy_merged_dict[text_to_index_dict[text]] = text
        # concat two dict
        clean_dict = dict(clean_dict, **duzzy_merged_dict)
    
    else:
        # Keep the shortest text in merged_list
        merged_dict, duplicated_dict = {}, {}
        for merged in tqdm(merged_list):
            merged_texts= [data_dict[idx] for idx in merged]
            min_text = min(merged_texts, key=len)
            min_index = text_to_index_dict[min_text]
            merged_dict[min_index] = min_text
            duplicated_dict[str(merged)] = merged_texts
    # save duplicated dict
    write_json(duplication_path, [duplicated_dict])
    # concat two dict
    print(len(list(clean_dict.keys())), len(list(merged_dict.keys())))
    clean_dict.update(merged_dict)
    # print deduplicated sample number
    print("deduplicated sample number: ", len(list(clean_dict.keys())))
    return clean_dict
    
# fuzzy deduplication
def fuzzy_deduplicated(text_list):
    # 保留得index
    save_list = []
    shreshold = 0.8
    for tl in tqdm(text_list):
        if len(tl) > 25:
            # Keep the shortest text
            min_index = tl.index(min(tl, key=len))
            save_list.append(tl[min_index])
            continue
        unique_texts, duplicate_texts = fuzzy_deduplicate(tl, shreshold)
        save_list.append(unique_texts)
    # flattened list
    save_list = list(chain.from_iterable(save_list))
    return save_list
    
# input id-text dict and text-value dict to get save list
def get_save_list(text_to_value_dict, clean_dict):
    save_list = []
    for key in tqdm(list(clean_dict.keys())):
        value = text_to_value_dict[clean_dict[key]]
        save_list.append(value)
    return save_list
# input sim_merged_list/min_merged_list, text_down_list/text_up_list, text_to_index_dict, output real sim_merged_list/min_merged_list
def get_hash_index(merged_list, text_list, text_to_index_dict):
    hash_index = []
    for value in tqdm(merged_list):
        index_list = []
        for index in value:
            text = text_list[index]
            real_index = text_to_index_dict[text]
            index_list.append(real_index)
        hash_index.append(index_list)
    return hash_index

def remove_gzh_regxy_match(text_dict):
    weixinhao_pattern1 = r'微信号\s\w+\s?'
    weixinhao_pattern2 = r'\w*\s微信号\s\w+\s?'
    date_pattern = r'\d{4}-\d{2}-\d{2}\s\w*'
    phone_pattern = r'(联系电话:\s?\d*-?\d*|联系电话：\s?\d*-?\d*)'
    mail_pattern = r'\w*邮箱:\s?[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}|\w*邮箱：\s?[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}'
    web_pattern = r'\w*网址:\s?[A-Za-z0-9\-.,#\/\s]+|\w*网址：\s?[A-Za-z0-9\-.,#\/\s]+'
    from_pattern = r'\w*{word}:\s?\w+|\w*{word}：\s?\w+'
    filter_words = ["来源", "转载", "以上内容来自", "版权说明", "作者"]
    sys_pattern = r'[?.,;:!，。？；：！、]{2,}' # 连续不同的符号
    # 括号中的内容匹配
    brackets_pattern = r'[(（【\[].+?[)）\]】]'
    begin_pattern = r'^\w+\s?\|\s?'
    space_pattern = r'\s(\S)\s'
    new_dict = {}
    for key in tqdm(list(text_dict.keys())):
        text = text_dict[key]
        text1 = re.sub(weixinhao_pattern2, "", text)
        text1 = clean_space(text1)
        text2 = re.sub(weixinhao_pattern1, "", text1)
        text2 = clean_space(text2)
        text3 = re.sub(date_pattern, "", text2)
        text3 = clean_space(text3)
        text4 = re.sub(phone_pattern, "", text3)
        text4 = clean_space(text4)
        text5 = re.sub(mail_pattern, "", text4)
        text5 = clean_space(text5)
        text6 = re.sub(web_pattern, "", text5)
        text6 = clean_space(text6)
        for word in filter_words:
            text6 = re.sub(from_pattern.format(word=word), "", text6)
            text6 = clean_space(text6)
        text7 = remove_consecutive_symbols(text6)
        text7 = clean_space(text7)
        text8 = re.sub(brackets_pattern, "", text7)
        text8 = clean_space(text8)
        text9 = re.sub(begin_pattern, "", text8)
        text9 = clean_space(text9)
        text10 = re.sub(space_pattern, "", text9)
        text10 = clean_space(text10)
        si = re.findall(sys_pattern, text10)
        if len(si) > 0:
            for s in si:
               text10 = text10.replace(s, s[0])
        text10 = clean_space(text10)
        new_dict[key] = text10
    return new_dict

def remove_en_regxy_match(text_dict):
    new_dict = {}
    sys_pattern = r'[?.,;:!，。？；：！、]{2,}'
    brackets_pattern = r'[(（【\[].+?[)）\]】]'
    for key in tqdm(list(text_dict.keys())):
        text = text_dict[key]
        text = remove_consecutive_symbols(text)
        text = remove_start_symbol(text)
        text = remove_email(text)
        text = re.sub(brackets_pattern, "", text)
        si = re.findall(sys_pattern, text)
        if len(si) > 0:
            for s in si:
               text = text.replace(s, s[0])
        new_dict[key] = text
    return new_dict
def remove_zh_regxy_match(text_dict):
    new_dict = {}
    sys_pattern = r'[?.,;:!，。？；：！、]{2,}'
    brackets_pattern = r'[(（【\[].+?[)）\]】]'
    page1_pattern = r'第\d{1,}页'
    page2_pattern = r'共\d{1,}页'
    dol_pattern = r'DOI:\w+'
    for key in tqdm(list(text_dict.keys())):
        text = text_dict[key]
        text = remove_consecutive_symbols(text)
        text = remove_start_symbol(text)
        text = remove_email(text)
        # text = re.sub(dol_pattern, "", text)
        # text = clean_space(text)
        text = re.sub(brackets_pattern, "", text)
     
        text = re.sub(page1_pattern, "", text)
  
        text = re.sub(page2_pattern, "", text)

        si = re.findall(sys_pattern, text)
        if len(si) > 0:
            for s in si:
               text = text.replace(s, s[0])

        new_dict[key] = text
    return new_dict
def remove_network_match(text):
    pattern = r"（.+）$"
    sys_pattern = r'[?.,;:!，。？；：！、]{2,}'
    text = re.sub(pattern, "", text)
    text = re.sub(sys_pattern, "", text)
    text = remove_email(text)
    text = remove_ip(text)
    text = remove_phone(text)
    text = remove_all_numbers(text)
    return text.strip()
def remove_langchao_match(text):
    text = re.sub(r'<n>', r'\n', text)
    # 删除空括号
    text = re.sub(r'\(\s*\)', "", text)
    text = re.sub(r'\（\s*\）', "", text)
    # 删除括号中的固定内容
    text = re.sub(r'\(律伴号:\w+\)', "", text)
    text = re.sub(r'\（记者\w+\）', "", text)
    text = text.strip()
    # text = re.sub(r'\(\w+\)$', "", text)
    # 按\n分割，如果最后一句以。/!/?/…结尾,不做处理，否则删除最后一句
    text_list = text.split("\n")
    last_text = text_list[-1]
    if len(re.findall(r"\w", last_text[-1])) > 0 or last_text[-1] in ["。", "!", "?", "…",".", "！","？"]:
        text = "\n".join(text_list)
    else:
        text = "\n".join(text_list[:-1])
    text = text.strip()
    return text

# split texts by space
def split_texts_by_space(text_dict):
    new_list = []
    for key in tqdm(list(text_dict.keys())):
        text = text_dict[key]
        text_list = text.split(" ")
        for i in range(len(text_list)):
            sentence = text_list[i]
            new_list.append({"id": key,
                             "index": str(i),
                             "text": sentence})
    return new_list

# split texts by dot
def split_texts_by_dot(text_dict):
    new_list = []
    for key in tqdm(list(text_dict.keys())):
        text = text_dict[key]
        text_list = text.split(".")
        for i in range(len(text_list)):
            sentence = text_list[i]
            new_list.append({"id": key,
                             "index": str(i),
                             "text": sentence})
    return new_list
# match keywords
def match_keywords(text_list, keywords):
    keywords = sorted(keywords, key=lambda x:len(x), reverse=True)
    for value in tqdm(text_list):
        for keyword in keywords:
            if keyword in value["text"]:
                value["is_match"] = True
                value["keyword"] = keyword
        if "is_match" not in list(value.keys()):
            value["is_match"] = False
            value["keyword"] = ""
    return text_list

# match keywords by text
def match_references_keywords(text_list):
    for value in tqdm(text_list):
        if "References" in value["text"]:
            value["is_match"] = True
            value["keyword"] = "References"
        elif re.match(r'\d{4}\.', value["text"]):
            value["is_match"] = True
            value["keyword"] = "years"
        elif re.match(r'\d{1,}\-\d{1,}', value["text"]):
            value["is_match"] = True
            value["keyword"] = "pages"
        else:
            value["is_match"] = False
            value["keyword"] = ""
    return text_list
# concat sentences with keywords
def merge_sentence(text_list):
    # split text_dict by id
    id_to_text_dict = {}
    for value in tqdm(text_list): 
        id = value["id"]
        if id not in list(id_to_text_dict.keys()):
            id_to_text_dict[id] = [value]
        else:
            id_to_text_dict[id].append(value)
    return id_to_text_dict
# delete reference before and after
def merge_sentence_by_bool(id_to_text_dict):
    new_dict = {}
    for key in tqdm(list(id_to_text_dict.keys())):
        text_dict_list = id_to_text_dict[key]
        index_to_bool_list = [] # make a index, is_match list
        index_to_text_dict = {} # make a index-text dict
        for text_dict in text_dict_list:
            text = text_dict["text"]
            index = text_dict["index"]
            index_to_text_dict[index] = text
            index_to_bool_list.append((int(index), text_dict["is_match"]))
        sorted_index_to_bool_list = sorted(index_to_bool_list, key=lambda x:x[0])
        try:
            start_index, end_index = find_index(sorted_index_to_bool_list)
            if start_index >= end_index:
                new_text = " "
                new_dict[key] = new_text
            else:
                new_list = []
                for i in range(start_index, end_index+1):
                    new_list.append(index_to_text_dict[str(i)])
                new_text = " ".join(new_list)
                new_text = clean_space(new_text)
                new_dict[key] = new_text
        except:
            new_list = []
            for text_dict in text_dict_list:
                text = text_dict["text"]
                new_list.append(text)
            new_text = " ".join(new_list)
            new_text = clean_space(new_text)
            new_dict[key] = new_text 
    return new_dict
# concat sentences with keywords delete reference after text
def merge_sentence_by_reference(id_to_text_dict):
    new_dict = {}
    for key in tqdm(list(id_to_text_dict.keys())):
        text_dict_list = id_to_text_dict[key]
        index_to_bool_list = [] # make a index, is_match list
        index_to_text_dict = {} # make a index-text dict
        for text_dict in text_dict_list:
            text = text_dict["text"]
            index = text_dict["index"]
            index_to_text_dict[index] = text
            index_to_bool_list.append((int(index), text_dict["is_match"], text_dict["keyword"]))
        sorted_index_to_bool_list = sorted(index_to_bool_list, key=lambda x:x[0])
        index = find_reference_index(sorted_index_to_bool_list)
        if index != -1:
            new_list = []
            for i in range(index):
                new_list.append(index_to_text_dict[str(i)])
        else:
            new_list = []
            for i in range(len(sorted_index_to_bool_list)):
                new_list.append(index_to_text_dict[str(i)])
        new_text = ".".join(new_list)
        new_text = clean_space(new_text)
        new_dict[key] = new_text
    return new_dict      
        
# find start and end index     
def find_index(sorted_list):
    false_index = []
    for i in range(len(sorted_list)):
        if sorted_list[i][1] == False:
            false_index.append(i)
    start = min(false_index)
    end = max(false_index)
    return start, end
def find_reference_index(sorted_list):
    false_index, ref_index = [], -1
    for i in range(len(sorted_list)):
        if sorted_list[i][1] == False:
            false_index.append(i)
        if sorted_list[i][2] == "References":
            ref_index = i
    # res_index location
    if ref_index == -1:
        return -1
    else:
        return ref_index
# replace attach sentence
def replace_wordninja(text):
    text_list = re.findall(r'[a-zA-Z]{10,}', text)
    replace_list = []
    for t in text_list:
        replace_list.append(split_wordninja(t))
    # print(text_list)
    replace_text = text
    for i in range(len(text_list)):
        replace_text = replace_text.replace(text_list[i], " ".join(replace_list[i]),1)
    return replace_text
# delete special unicode
def remove_special_unicode(text_dict):
    new_dict = {}
    print("start delete special unicode")
    for key in tqdm(list(text_dict.keys())):
        text = text_dict[key]
        res = emoji.replace_emoji(text, replace="")
        new_text = remove_unicode_fixed(res)
        new_dict[key] = new_text
    print("delete special unicode finished!")
    return new_dict
def remove_special_unicode2(text):
    res = emoji.replace_emoji(text, replace="")
    new_text = remove_unicode_fixed(res)
    return new_text
# delete redundant space and line
def remove_dup_space_and_line(text_dict):
    print("start delete redundant space and line")
    new_dict = {}
    for key in tqdm(list(text_dict.keys())):
        text = text_dict[key]
        new_text = remove_consecutive_symbols(text)
        new_dict[key] = new_text
    print("delete redundant space and line finished!")
    return new_dict
# delete short text below 50
def remove_short_text(text_dict):
    print("start delete short text below 50")
    new_dict = {}
    for key in tqdm(list(text_dict.keys())):
        text = text_dict[key]
        if len(text) < 50:
            continue
        else:
            new_dict[key] = text
    print("delete short text below 50 finished!")
    return new_dict
# split text to list by space
def split_en_text_by_space(text_dict):
    new_dict = {}
    for key in tqdm(list(text_dict.keys())): 
        text = text_dict[key]
        text_list = text.split(" ")
        new_dict[key] = text_list
    return new_dict
# Resolve text adhesion
def split_attach_text(text_dict):
    new_dict = {}
    print("start resolve text adhesion")
    for key in tqdm(list(text_dict.keys())):
        text_list = text_dict[key]
        new_text_list = []
        for text in text_list:
            text = text.strip()
            if is_all_only_english(text):
                new_text_list.append(split_wordninja(text))
            else:
                other_list = split_string(text)
                for other in other_list:
                    if is_all_only_english(other):
                        new_text_list.append(split_wordninja(other))
                    else:
                        new_text_list.append(other)
        # flatten list
        new_text_list = list(chain.from_iterable(new_text_list))
        new_text = " ".join(new_text_list)
        new_text = re.sub(r'\s+([^\w\s]\s+)', r'\1', new_text)
        new_text = re.sub(r'(\d+)\s+(\d+)', r'\1\2', new_text)
        new_text = re.sub(r'\-\s+(\d+)', r'-\1', new_text)
        new_dict[key] = clean_space(new_text)
    print("resolve text adhesion finished!")
    return new_dict
# Splitting text based on Chinese and English symbols and resolving adhesions
def split_zh_en_attach_text(text_dict):
    new_dict = {}
    print("start resolving adhesions")
    for key in tqdm(list(text_dict.keys())):
        text= text_dict[key]
        new_list = []
        text_list = split_string(text)
        for string in text_list:
            if is_all_only_english(string):
                new_text_list = split_wordninja(string)
                new_text = " ".join(new_text_list)
                new_list.append(new_text)
            else:
                new_list.append(string)
        new_text = "".join(new_list)
        new_dict[key] = clean_space(new_text)
    print("resolving adhesions finished!")
    return new_dict
# Calculate the number of tokens
def count_tokens(text, tokenizer):
    input = get_input_ids(text, tokenizer)
    return len(input)
# Calculate the number of tokens for a list
def count_tokens_list(text_list, tokenizer):
    all_nums = 0
    for text in tqdm(text_list):
        all_nums += count_tokens(text, tokenizer)
    return all_nums

# clean text 
def clean_text(text):
    text = text.strip()
    text = remove_special_unicode2(text)
    text = remove_consecutive_symbols(text)
    new_text = remove_start_symbol(text)
    return new_text

# clean_text_customed
def clean_text_customed(text):
    
    return text