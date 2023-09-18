
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import numpy as np 
import fasttext
from utils import *


def train(model_path, save_path):
    model = fasttext.train_supervised(input="/home/xuhao/xcxhy/Focus_Classification/dataset/meaning/quality_train.txt", wordNgrams=5)

    model.save_model("/home/xuhao/xcxhy/Focus_Classification/model/judge_content_quality_120k_v3.bin")

if __name__=="__main__":
    model_path = "/home/xuhao/xcxhy/Focus_Classification/model/judge_content_quality_120k_v3.bin"
    save_path = "/home/xuhao/xcxhy/Focus_Classification/model"
    train(model_path, save_path)