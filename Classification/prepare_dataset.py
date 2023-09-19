import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import numpy as np 
import fasttext
from utils import *
from Clean.clean_content import find_bad_sentence


if __name__=="__main__":
    # read data
    normal_path = ""
    trade_path = ""
    normal_data = read_json(normal_path)
    trade_data = read_json(trade_path)

    
    negative_data, positive_data = [], []
    for value in tqdm(normal_data):
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
    # put negative data write in negative_data.txt
    with open("negative.txt", "w") as f:
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
    # put positive data write in positive_data.txt
    with open("positive.txt", "w") as f:
        for text in positive_data[:10000]:
            f.write(text + "\n")
    # concat two list
    all_data = negative_data[:10000] + positive_data[:10000]
    print("all_data: ", len(all_data))
    
    # shuffle the data
    np.random.shuffle(all_data)
    
    # split train and test dataset
    train_data = all_data[:int(len(all_data)*0.8)]
    test_data = all_data[int(len(all_data)*0.8):]
    
    # write file
    with open("train.txt", "w") as f:
        for text in train_data:
            f.write(text + "\n")
    with open("test.txt", "w") as f:
        for text in test_data:
            f.write(text + "\n")

    
    
    
            