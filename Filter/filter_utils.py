import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import *
from itertools import chain, repeat
from multiprocessing import Pool
from concurrent import futures
# import fasttext

def read_file_or_dir(path):
    # start read file
    # judge file or dir
    if os.path.isfile(path):
        data = read_json(path)
        # get part data
        # data = data[:1000]
        cate = choose_file_category(path)
        return data, cate
    elif os.path.isdir(path):
        data = []
        files = os.listdir(path)
        for file in tqdm(files):
            if file.endswith(".json") or file.endswith(".jsonl"):
                file_path = os.path.join(path, file)
                value = read_json(file_path)
                data.extend(value)
        cate = choose_file_category(os.path.join(path, files[0]))
        return data, cate
    else:
        raise ValueError("path is not a file or dir")

def filter_advertisement_text(data, workers):
    
    
    return 

def filter_messy_code_text(data):
    """
    去除乱码
    """
    start = time.time()
    positive_list, negative_list = fasttext_format_inference_single(data)
    end = time.time()
    print("time: ", end-start)
    print("negative_list: ", len(negative_list))
    # write_json("/home/xuhao/xcxhy/Focus_Filter/result/negative_list.json", negative_list)
    # write_json("/home/xuhao/xcxhy/Focus_Filter/result/trade_networks_positive_list.json", positive_list[10000:20000])
    
    return positive_list

def fasttext_format_inference_single(text_dict):
    model = fasttext.load_model("/home/xuhao/xcxhy/Focus_Classification/model/judge_content_quality_120k_v3.bin")
    positive_list, negative_list = [], []
    # result = model.predict([value["text"] for value in text_dict])
    # for i in tqdm(range(len(result[0]))):
    #     val = result[0][i]
    #     predict = val[0]
    #     if predict == "__label__positive":
    #         positive_list.append(text_dict[i])
    #     else:
    #         negative_list.append(text_dict[i])
    for value in tqdm(text_dict):
        text = value["text"]
        text = text.replace("\n", "")
        score = model.predict(text)
        predict = score[0][0]
        if predict == "__label__positive":
            positive_list.append(value)
        else:
            negative_list.append(value)
    return positive_list, negative_list


def filter_short_text(text_dict, tokenizer_path, workers):
    """
    过滤短文本
    """
    tokenizer = get_tokenizer(tokenizer_path) # initial tokenizer
    if workers == 1:
        start = time.time()
        long_data, short_data, nums_200, nums_500, nums_1000, nums_2000, nums_4000, nums_up = simulate_tokens(text_dict, tokenizer)
        end = time.time()
        print("time: ", end-start)
    else:
        start = time.time()
        long_data, short_data, nums_200, nums_500, nums_1000, nums_2000, nums_4000, nums_up = [], [], 0, 0, 0, 0, 0, 0
        # with futures.ThreadPoolExecutor(workers) as executor:
        #     result = executor.map(simulate_tokens_single, text_dict, repeat(tokenizer))
        #     result = list(result)
        pool = Pool(workers)
        chunk_size = len(text_dict)//pool._processes
        result = pool.starmap(simulate_tokens_single, zip(text_dict, repeat(tokenizer)), chunksize=chunk_size)
        pool.close()
        pool.join()
        for value, length in tqdm(result):
            if length>=50:
                long_data.append(value)
                if length <=200:
                    nums_200 += 1
                elif length <=500 and length>200:
                    nums_500 += 1
                elif length <=1000 and length>500:
                    nums_1000 += 1
                elif length <=2000 and length>1000:
                    nums_2000 += 1
                elif length <=4000 and length>2000:
                    nums_4000 += 1
                else:
                    nums_up += 1
            else:
                short_data.append(value)
        end = time.time()
        print("time: ", end-start)
        
    print("nums_200: ", nums_200)
    print("nums_500: ", nums_500)
    print("nums_1000: ", nums_1000)
    print("nums_2000: ", nums_2000)
    print("nums_4000: ", nums_4000)
    print("nums_up: ", nums_up)
    return long_data, short_data

def simulate_tokens(data, tokenizer):
    nums_200, nums_500, nums_1000, nums_2000, nums_4000, nums_up = 0, 0, 0, 0, 0, 0
    long_data, short_data = [], []
    for value in tqdm(data):
        text = value['text']
        length = len(get_input_ids(text, tokenizer))
        if length>=50:
            long_data.append(value)
            if length <=200:
                nums_200 += 1
            elif length <=500 and length>200:
                nums_500 += 1
            elif length <=1000 and length>500:
                nums_1000 += 1
            elif length <=2000 and length>1000:
                nums_2000 += 1
            elif length <=4000 and length>2000:
                nums_4000 += 1
            else:
                nums_up += 1
        else:
            short_data.append(value)
    return long_data, short_data, nums_200, nums_500, nums_1000, nums_2000, nums_4000, nums_up

def simulate_tokens_single(data, tokenizer):
    text = data['text']
    length = len(get_input_ids(text, tokenizer))
    return data, length


