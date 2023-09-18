import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import numpy as np 
import fasttext
from utils import *
from Clean.clean_content import find_bad_sentence


if __name__=="__main__":
    # 读取数据
    data = read_json("/home/xuhao/xcxhy/Focus_Dataset/data/trade_bbs_08_29.json")
    trade_data = read_json("/home/xuhao/xcxhy/Focus_Dataset/data/foreign_trade_08_23_merge_content_clean.json")
    
    negative_data, positive_data = [], []
    for value in tqdm(data):
        try:
            text = value["question"]
            if len(text) > 1000:
                continue
            if find_bad_sentence(text):
                negative_data.append("__label__negative" + "\t" +text.replace("\n", ""))
            else:
                pass 
        except:
            continue
    print("negative_data: ", len(negative_data))
    negative_data = list(set(negative_data))
    print("negative_data: ", len(negative_data))
    # 将negative data 写入negative_data.txt中
    with open("/home/xuhao/xcxhy/Focus_Classification/dataset/format/negative.txt", "w") as f:
        for text in negative_data[:10000]:
            f.write(text + "\n")
    
    for value in tqdm(trade_data):
        try:
            text = value['text']
            if len(text) > 500:
                if "。" in text:
                    text_list = text.split("。")
                    positive_data.append("__label__positive"+ "\t" + "。".join(text_list[:5]).replace("\n", ""))
                else:
                    text_list = text.split(".")
                    positive_data.append("__label__positive"+ "\t" + ".".join(text_list[:5]).replace("\n", ""))
            else:
                positive_data.append("__label__positive" + "\t" +text.replace("\n", ""))
        except:
            continue
    print("positive_data: ", len(positive_data))
    positive_data = list(set(positive_data))
    print("positive_data: ", len(positive_data))
    # 将positive data 写入positive_data.txt中
    with open("/home/xuhao/xcxhy/Focus_Classification/dataset/format/positive.txt", "w") as f:
        for text in positive_data[:10000]:
            f.write(text + "\n")
    # 合并两个list
    all_data = negative_data[:10000] + positive_data[:10000]
    print("all_data: ", len(all_data))
    
    # 打乱数据
    np.random.shuffle(all_data)
    
    # 划分数据集
    train_data = all_data[:int(len(all_data)*0.8)]
    test_data = all_data[int(len(all_data)*0.8):]
    
    # 写入文件
    with open("/home/xuhao/xcxhy/Focus_Classification/dataset/format/train.txt", "w") as f:
        for text in train_data:
            f.write(text + "\n")
    with open("/home/xuhao/xcxhy/Focus_Classification/dataset/format/test.txt", "w") as f:
        for text in test_data:
            f.write(text + "\n")

    
    
    
            