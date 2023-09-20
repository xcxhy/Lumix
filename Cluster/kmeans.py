'''
Author: your name
Date: 2023-09-20 17:50:23
LastEditTime: 2023-09-20 18:02:50
LastEditors: xuhao0101
Description: In User Settings Edit
FilePath: \Lumix\Cluster\kmeans.py
'''

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import numpy as np 
import argparse
from sklearn.cluster import KMeans, AgglomerativeClustering
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import matplotlib.pyplot as plt
from utils import *
from sentence_transformers import SentenceTransformer

def kmeans():
    
    return 


if __name__=="__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data_path", type=str, default="/Data/store/all_web_cleaned_8.json")
    parser.add_argument("--model_path", type=str, default="all-MiniLM-L6-v2")
    parser.add_argument("--save_path", type=str, default="/Data/store")
    parser.add_argument("--n_clusters", type=int, default=2)
    
    args = parser.parse_args()
    
    # read data
    data_list = []
    data = read_json(args.data_path)
    
    for value in tqdm(data):
        data_list.append(value["text"])
    
    result = kmeans()