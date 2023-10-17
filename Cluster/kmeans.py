
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

def kmeans(args):
    model = SentenceTransformer(args.model_path)
    
    data_list = []
    for value in tqdm(data):
        data_list.append(value["text"])
    corpus_embeddings = model.encode(data_list)
    
    # K_means clustering
    clustering_model = KMeans(n_clusters=args.n_clusters)
    clustering_model.fit(corpus_embeddings)
    cluster_assignment = clustering_model.labels_
    # get cluster sentences
    clustered_sentences = [[] for i in range(args.n_clusters)]
    for sentence_id, cluster_id in enumerate(cluster_assignment):
        clustered_sentences[cluster_id].append(data_list[sentence_id])
    # get cluster center
    for key in clustered_sentences:
        length = len(key)
        write_json(os.path.join(args.save_dir, str(length) + ".json"), [{str(length): key}])
        
    return 


if __name__=="__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data_path", type=str, default="Data/store/webs/trade_webs_cleaned_8_unique.json")
    parser.add_argument("--model_path", type=str, default="Model/all-MiniLM-L6-v2")
    parser.add_argument("--save_dir", type=str, default="Data/store/cluster")
    parser.add_argument("--n_clusters", type=int, default=2)
    args = parser.parse_args()
    
    # read data
    data_list = []
    data = read_json(args.data_path)
    result = kmeans(args)