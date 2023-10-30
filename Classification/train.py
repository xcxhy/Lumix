
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import numpy as np 
import fasttext
from utils import *


def train(text_path, model_path):
    model = fasttext.train_supervised(input=text_path, wordNgrams=5)

    model.save_model(model_path)

if __name__=="__main__":
    text_path = "Data/meaning/quality_train.txt"
    model_path = "/home/xuhao/xcxhy/Focus_Classification/model"
    train(text_path, model_path)