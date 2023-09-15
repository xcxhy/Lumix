import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import numpy as np 
from utils import *
import re
from tqdm import tqdm
from itertools import chain
from datasketch import MinHash, MinHashLSH
import jieba
import random
import Levenshtein

def fuzzy_deduplicate(text_list, shreshold):
    unique_texts, duplicate_texts = [],[]
    for text in text_list:
        is_deplicate = False
        for unique_text in unique_texts:
            distance = Levenshtein.distance(text, unique_text)
            similarity = (len(text) - distance) / len(text)
            if similarity >= shreshold:
                is_deplicate = True
                duplicate_texts.append([text, unique_text])
                break
        if not is_deplicate:
            unique_texts.append(text)
    return unique_texts, duplicate_texts     

