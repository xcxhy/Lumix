import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import numpy as np 
import argparse
from sklearn.cluster import AgglomerativeClustering
import matplotlib.pyplot as plt
from utils import *
from sentence_transformers import SentenceTransformer

def agglomerative(args):
    model = SentenceTransformer(args.model_path)
    
    data_list = []
    for value in tqdm(data):
        data_list.append(value["text"])
        
    corpus_embeddings = model.encode(data_list)
    corpus_embeddings = corpus_embeddings / np.linalg.norm(corpus_embeddings, axis=1, keepdims=True)
    
    # Agglomerative clustering
    clustering_model = AgglomerativeClustering(n_clusters=None, distance_threshold=5)
    clustering_model.fit(corpus_embeddings)
    cluster_assignment = clustering_model.labels_
    
    # get cluster sentences
    clustered_sentences = {}
    for sentence_id, cluster_id in enumerate(cluster_assignment):
        if cluster_id not in clustered_sentences:
            clustered_sentences[cluster_id] = []
        clustered_sentences[cluster_id].append(data_list[sentence_id])
        
        clustered_sentences[cluster_id].append(data_list[sentence_id])
    
    # clean dir
    if os.path.exists(args.save_dir):
        shutil.rmtree(args.save_dir)
    os.makedirs(args.save_dir)
    for key in list(clustered_sentences.keys()):
        clustered_list = clustered_sentences[key]
        new_clustered_list = []
        for text in clustered_list:
            new_clustered_list.append(text)
        write_json(os.path.join(args.save_dir, str(key) + ".json"), new_clustered_list)
    
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
    result = agglomerative(args)