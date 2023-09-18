import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import numpy as np 
import fasttext
from utils import *

# 定义二分类数据函数
def binary_dataset(neg_data, pos_data, save_path):
    negative_data, positive_data = [], []
    for text1 in neg_data:
        negative_data.append("__label__negative" + "\t" +text1.replace("\n",""))
    for text2 in pos_data:
        positive_data.append("__label__positive" + "\t" +text2.replace("\n",""))
    
    train_data, validation_data = [], []
    # 按照8:2划分数据集
    train_data.extend(negative_data[:int(len(negative_data)*0.8)])
    train_data.extend(positive_data[:int(len(positive_data)*0.8)])
    validation_data.extend(negative_data[int(len(negative_data)*0.8):])
    validation_data.extend(positive_data[int(len(positive_data)*0.8):])
    print("train_data: ", len(train_data))
    print("validation_data: ", len(validation_data))
    # 打乱数据
    np.random.shuffle(train_data)
    np.random.shuffle(validation_data)
    # 写入数据
    with open(save_path + "/quality_train.txt", "w") as f:
        for text in train_data:
            f.write(text + "\n")
    with open(save_path + "/quality_validation.txt", "w") as f:
        for text in validation_data:
            f.write(text + "\n")
            
    
    

if __name__=="__main__":
    data = read_json("/home/xuhao/xcxhy/Focus_Classification/dataset/meaning/trade_class_zh_clean.json")
    negative_data, positive_data = [], []
    for value in tqdm(data):
        text = value["text"]
        text = text.replace("\n", "")
        text = text.replace("\r", "")
        label = value["label"]
        if label == 1:
            positive_data.append(text)
        else:
            negative_data.append(text)
    print("negative_data: ", len(negative_data))
    print("positive_data: ", len(positive_data))
    # 二分类数据
    binary_dataset(negative_data, positive_data, "/home/xuhao/xcxhy/Focus_Classification/dataset/meaning")
    save_path = "/home/xuhao/xcxhy/Focus_Classification/dataset/meaning"
    with open(save_path + "/quality_train.txt", "r") as f:
        train_data = f.readlines()
        print("train_data: ", len(train_data))
    with open(save_path + "/quality_validation.txt", "r") as f:
        validation_data = f.readlines()
        print("validation_data: ", len(validation_data))
    # 删除
    # new_train_data, new_validation_data = [],[]
    # for train in train_data:
    #     if "__label__positive" in train or "__label__negative" in train:
    #         new_train_data.append(train.strip())
    # for validation in validation_data:
    #     if "__label__positive" in validation or "__label__negative" in validation:
    #         new_validation_data.append(validation.strip())
    # print("new_train_data: ", len(new_train_data))
    # print("new_validation_data: ", len(new_validation_data))
    
    # with open(save_path + "/quality_train.txt", "w") as f:
    #     for text in new_train_data:
    #         f.write(text + "\n")
    # with open(save_path + "/quality_validation.txt", "w") as f:
    #     for text in new_validation_data:
    #         f.write(text + "\n")