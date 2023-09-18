import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import numpy as np 
import fasttext
from utils import *


def fasttext_content_inference(text_dict, path):
    model = fasttext.load_model(path)
    new_dict = {}
    for key, text in text_dict.items():
        score = model.predict(text)
        predict = score[0][0]
        if predict == "__label__positive":
            new_dict[key] = text
    return new_dict


