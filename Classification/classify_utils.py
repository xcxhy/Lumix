import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import *
import torch
from sentence_transformers import SentenceTransformer, util

def sample_classify(text_list, labels, model):
    corpus = labels
    #Encode corpus
    corpus_embeddings = model.encode(corpus, convert_to_tensor=True)
    
    #Encode queries
    queries = text_list
    query_embeddings = model.encode(queries, convert_to_tensor=True)
    
    #Compute cosine-similarits
    result = []
    for index in tqdm(range(len(query_embeddings))):
        
        cos_scores = util.pytorch_cos_sim(query_embeddings[index], corpus_embeddings)[0]
        cos_scores = cos_scores.cpu()
        
        #We use torch.topk to find the highest 5 scores
        top_results = torch.topk(cos_scores, k=3)
        
        # print("\n\n======================\n\n")
        # print("Query:", queries[index])
        # print("\nTop 5 most similar sentences in corpus:")
        res_labels, res_scores = [], []
        for score, idx in zip(top_results[0], top_results[1]):
            # print(labels[idx], "(Score: {:.4f})".format(score))
            res_labels.append(labels[idx])
            res_scores.append(round(float(score), 4))
        result.append({"text": queries[index], "labels": res_labels, "scores": res_scores})
    return result

def sample_classify_v2(text_list, labels, model):
    corpus = labels
    #Encode corpus
    
    
    #Encode queries
    queries = text_list

    pool = model.start_multi_process_pool()
    corpus_embeddings = model.encode_multi_process(corpus, pool)
    query_embeddings = model.encode_multi_process(queries, pool)
    model.stop_multi_process_pool(pool)
    #Compute cosine-similarits
    result = []
    for index in tqdm(range(len(query_embeddings))):
        # query_embeddings = model.encode(query, convert_to_tensor=True)
        cos_scores = util.pytorch_cos_sim(query_embeddings[index], corpus_embeddings)[0]
        cos_scores = cos_scores.cpu()
        
        #We use torch.topk to find the highest 5 scores
        top_results = torch.topk(cos_scores, k=3)
        
        # print("\n\n======================\n\n")
        # print("Query:", queries[index])
        # print("\nTop 5 most similar sentences in corpus:")
        res_labels, res_scores = [], []
        for score, idx in zip(top_results[0], top_results[1]):
            # print(labels[idx], "(Score: {:.4f})".format(score))
            res_labels.append(labels[idx])
            res_scores.append(round(float(score), 4))
        result.append({"text": queries[index], "labels": res_labels, "scores": res_scores})
    return result

def piccolo_classify(text_list, labels, model):
    corpus = ["结果："+label for label in labels]
    # corpus = [label for label in labels]
    #Encode corpus
    corpus_embeddings = model.encode(corpus, convert_to_tensor=True)
    
    #Encode queries
    queries = ["查询："+ txt for txt in text_list]
    # queries = [txt for txt in text_list]
    query_embeddings = model.encode(queries, convert_to_tensor=True)
    
    #Compute cosine-similarits
    result = []
    for index in tqdm(range(len(query_embeddings))):
        
        cos_scores = util.pytorch_cos_sim(query_embeddings[index], corpus_embeddings)[0]
        cos_scores = cos_scores.cpu()
        
        #We use torch.topk to find the highest 5 scores
        top_results = torch.topk(cos_scores, k=3)
        
        # print("\n\n======================\n\n")
        # print("Query:", text_list[index])
        # print("\nTop 5 most similar sentences in corpus:")
        res_labels, res_scores = [], []
        for score, idx in zip(top_results[0], top_results[1]):
            # print(labels[idx], "(Score: {:.4f})".format(score))
            res_labels.append(labels[idx])
            res_scores.append(round(float(score), 4))
        result.append({"text": text_list[index], "labels": res_labels, "scores": res_scores})
    return result

def piccolo_classify_v2(text_list, labels, model):
    corpus = ["结果："+label for label in labels]
    # corpus = [label for label in labels]
    #Encode corpus
    
    #Encode queries
    queries = ["查询："+ txt for txt in text_list]
    # queries = [txt for txt in text_list]
    pool = model.start_multi_process_pool()
    corpus_embeddings = model.encode_multi_process(corpus, pool)
    query_embeddings = model.encode_multi_process(queries, pool)
    model.stop_multi_process_pool(pool)
    
    #Compute cosine-similarits
    result = []
    for index in tqdm(range(len(query_embeddings))):
        
        cos_scores = util.pytorch_cos_sim(query_embeddings[index], corpus_embeddings)[0]
        cos_scores = cos_scores.cpu()
        
        #We use torch.topk to find the highest 5 scores
        top_results = torch.topk(cos_scores, k=3)
        
        # print("\n\n======================\n\n")
        # print("Query:", text_list[index])
        # print("\nTop 5 most similar sentences in corpus:")
        res_labels, res_scores = [], []
        for score, idx in zip(top_results[0], top_results[1]):
            # print(labels[idx], "(Score: {:.4f})".format(score))
            res_labels.append(labels[idx])
            res_scores.append(round(float(score), 4))
        result.append({"text": text_list[index], "labels": res_labels, "scores": res_scores})
    return result

def bge_classify(texts, labels, model):
    
    instruction = "为这个文章生成表示以用于检索相关目录："
    
    corpus = labels
    corpus_embeddings = model.encode(corpus, convert_to_tensor=True)
    
    # Encode queries
    q_embeddings = model.encode([instruction+ q for q in texts], convert_to_tensor=True)
    
    # Compute cosine-similarits
    result = []
    for index in tqdm(range(len(q_embeddings))):
        cos_scores = util.pytorch_cos_sim(q_embeddings[index], corpus_embeddings)[0]
        cos_scores = cos_scores.cpu()
        
        # We use torch.topk to find the highest 5 scores
        top_results = torch.topk(cos_scores, k=3)
        
        # print("\n\n======================\n\n")
        # print("Query:", texts[index])
        # print("\nTop 3 most similar sentences in corpus:")
        res_labels, res_scores = [], []
        for score, idx in zip(top_results[0], top_results[1]):
            # print(labels[idx], "(Score: {:.4f})".format(score))
            res_labels.append(labels[idx])
            res_scores.append(round(float(score), 4))
        result.append({"text": texts[index], "labels": res_labels, "scores": res_scores})

    return result

def bge_classify_v2(texts, labels, model):
    
    instruction = "为这个文章生成表示以用于检索相关目录："
    
    corpus = labels
    # corpus_embeddings = model.encode(corpus, convert_to_tensor=True, normalize_embeddings=True)
    
    # Encode queries
    # q_embeddings = model.encode([instruction+ q for q in texts], normalize_embeddings=True, convert_to_tensor=True)
    pool = model.start_multi_process_pool()
    corpus_embeddings = model.encode_multi_process(corpus, pool)
    q_embeddings = model.encode_multi_process([instruction+ q for q in texts], pool)
    model.stop_multi_process_pool(pool)
    # Compute cosine-similarits
    result = []
    for index in tqdm(range(len(q_embeddings))):
        cos_scores = util.pytorch_cos_sim(q_embeddings[index], corpus_embeddings)[0]
        cos_scores = cos_scores.cpu()
        
        # We use torch.topk to find the highest 5 scores
        top_results = torch.topk(cos_scores, k=3)
        
        # print("\n\n======================\n\n")
        # print("Query:", texts[index])
        # print("\nTop 3 most similar sentences in corpus:")
        res_labels, res_scores = [], []
        for score, idx in zip(top_results[0], top_results[1]):
            # print(labels[idx], "(Score: {:.4f})".format(score))
            res_labels.append(labels[idx])
            res_scores.append(round(float(score), 4))
        result.append({"text": texts[index], "labels": res_labels, "scores": res_scores})

    return result