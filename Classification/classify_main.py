import os
# os.environ["CUDA_VISIBLE_DEVICES"]='0,1,2,3,4,5,6,7'
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from classify_utils import *
from utils import *
import argparse
from itertools import chain, repeat
from multiprocessing import Pool
from concurrent import futures
from collections import Counter
# import fasttext

def main(args):
    origin_labels = args.labels
    # unique_files_path
    files = os.listdir(args.data_path)
    data_paths = [os.path.join(args.data_path, file) for file in files]
    # read data
    for file in tqdm(files):
        print("start classify: ", file)
        if os.path.exists(os.path.join(args.output_dir, file)):
            print("file has been classified")
            continue
        data = read_json(os.path.join(args.data_path, file))
        data = data[:20000]
        if args.workers == 1:
            start = time.time()
            final_data = classify_single(data, origin_labels)
            end = time.time()
            print("all time: ", end-start)
        else:
            start = time.time()
            final_data = classify_multi(data, origin_labels, args.workers)
            end = time.time()
            print("all time: ", end-start)
        if not os.path.exists(args.output_dir):
            os.makedirs(args.output_dir)
        print("final_data: ", len(final_data))
        write_json(os.path.join(args.output_dir, file), final_data)
    
    
    return 

def classify_single(data, origin_labels):
    # initial data
    corpus = origin_labels
    ids = [value["unique_id"] for value in data]
    queries = [value["text"] for value in data]
    
    # initial model
    minilm_model = SentenceTransformer("Model\\paraphrase-multilingual-MiniLM-L12-v2")
    one_start = time.time()
    minilm_res = sample_classify(queries, corpus, minilm_model)
    one_end = time.time()
    print("one time: ", one_end-one_start)
    mpnet_model = SentenceTransformer("Model\\paraphrase-multilingual-mpnet-base-v2")
    two_start = time.time()
    mpnet_res = sample_classify(queries, corpus, mpnet_model)
    two_end = time.time()
    print("two time: ", two_end-two_start)
    bge_model = SentenceTransformer("Model\\bge-base-zh-v1.5")
    three_start = time.time()
    bge_res = bge_classify(queries, corpus, bge_model)
    three_end = time.time()
    print("three time: ", three_end-three_start)
    print("get the results!")
    # get the results
    
    # merge the results
    all_data = {}
    all_data["unique_id"] = ids
    all_data["text"] = queries
    all_data["minilm_labels"] = [value["labels"] for value in minilm_res]
    all_data["minilm_scores"] = [value["scores"] for value in minilm_res]
    all_data["mpnet_labels"] = [value["labels"] for value in mpnet_res]
    all_data["mpnet_scores"] = [value["scores"] for value in mpnet_res]
    all_data["bge_labels"] = [value["labels"] for value in bge_res]
    all_data["bge_scores"] = [value["scores"] for value in bge_res]
    
    choose_names = ["minilm_labels", "mpnet_labels", "bge_labels"]
    nums_res = []
    print("merge the results!")
    merge_start = time.time()
    # for i in tqdm(range(len(all_data["text"]))):
    #     labels, scores = [], []
    #     for name in choose_names:
    #         # each_scores = all_data[name.replace("labels", "scores")][i]
    #         # if each_scores[0] - each_scores[1] > 0.05:
    #         #     each_scores = [each_scores[0]]
    #         # elif each_scores[1] - each_scores[2] > 0.05:
    #         #     each_scores = each_scores[:2]
    #         # else:
    #         #     each_scores = each_scores
    #         labels.append(all_data[name][i][:1])
    #         # scores.append(each_scores)
    #     labels = list(chain.from_iterable(labels))
    #     # nums_r = Counter(labels).most_common(3)
    #     nums_r = list(set(labels))
    #     nums_res.append(nums_r)
    nums_res = counter_most_3(all_data["minilm_labels"], all_data["mpnet_labels"], all_data["bge_labels"])
    merge_end = time.time()
    print("merge time: ", merge_end-merge_start)
    # predict_labels = []
    # for value in nums_res:
    #     predict_labels.append([v for v in value])
    all_data["three_labels"] = nums_res
    
    final_data = []
    final_start = time.time()
    for i in tqdm(range(len(data))):
        value = data[i]
        ids = value["unique_id"]
        text = value["text"]
        meta = value["meta"]
        three_labels = all_data["three_labels"][i]
        
        meta["three_labels"] = three_labels
        final_data.append({"unique_id": ids, "text": text, "meta": meta})
    final_end = time.time()
    print("final time: ", final_end-final_start)
    return final_data

def counter_most_3(minilm_labels, mpnet_labels, bge_labels):
    all_labels = []
    for i in tqdm(range(len(minilm_labels))):
        labels = []
        labels.append(minilm_labels[i])
        labels.append(mpnet_labels[i])
        labels.append(bge_labels[i])
        labels = list(chain.from_iterable(labels))
        nums_r = Counter(labels).most_common(3)
        all_labels.append([v[0] for v in nums_r])
    return all_labels

def classify_multi(data, origin_labels, workers):
        # initial data
    corpus = origin_labels
    ids = [value["unique_id"] for value in data]
    queries = [value["text"] for value in data]
    N = len(ids)//workers
    multi_ids, multi_queries = [ids[i:i+N] for i in range(0, len(ids), N)], [queries[i:i+N] for i in range(0, len(queries), N)]
    
    
    pool = Pool(workers)
    chunk_size = len(queries) // pool._processes
    res = pool.starmap(classify_model, zip(multi_ids, multi_queries, repeat(corpus)), chunksize=chunk_size)
    pool.close()
    pool.join()
    for i in range(len(res)):
        if i == 0:
            f_dict = res[i]
        else:
            f_dict.update(res[i])
    
    final_data = []
    for value in tqdm(data):
        ids = value["unique_id"]
        text = value["text"]
        meta = value["meta"]
        three_labels = f_dict[ids]["labels"]
        meta["three_labels"] = three_labels
        final_data.append({"unique_id": ids, "text": text, "meta": meta})
    
    return final_data

def classify_model(ids, queries, corpus):
    # for value in all_queries:
    #     ids.append(value[0])
    #     queries.append(value[1])
    
    # initial model
    minilm_model = SentenceTransformer("Model\\paraphrase-multilingual-MiniLM-L12-v2")
    one_start = time.time()
    minilm_res = sample_classify(queries, corpus, minilm_model)
    one_end = time.time()
    print("one time: ", one_end-one_start)
    mpnet_model = SentenceTransformer("Model\\paraphrase-multilingual-mpnet-base-v2")
    two_start = time.time()
    mpnet_res = sample_classify(queries, corpus, mpnet_model)
    two_end = time.time()
    print("two time: ", two_end-two_start)
    bge_model = SentenceTransformer("Model\\bge-base-zh-v1.5")
    three_start = time.time()
    bge_res = bge_classify(queries, corpus, bge_model)
    three_end = time.time()
    print("three time: ", three_end-three_start)
    print("get the results!")
    # get the results
    
    # merge the results
    all_data = {}
    all_data["unique_id"] = ids
    all_data["text"] = queries
    all_data["minilm_labels"] = [value["labels"] for value in minilm_res]
    all_data["minilm_scores"] = [value["scores"] for value in minilm_res]
    all_data["mpnet_labels"] = [value["labels"] for value in mpnet_res]
    all_data["mpnet_scores"] = [value["scores"] for value in mpnet_res]
    all_data["bge_labels"] = [value["labels"] for value in bge_res]
    all_data["bge_scores"] = [value["scores"] for value in bge_res]
    
    choose_names = ["minilm_labels", "mpnet_labels", "bge_labels"]
    nums_res = []
    print("merge the results!")
    for i in tqdm(range(len(all_data["text"]))):
        labels, scores = [], []
        for name in choose_names:
            each_scores = all_data[name.replace("labels", "scores")][i]
            if each_scores[0] - each_scores[1] > 0.05:
                each_scores = [each_scores[0]]
            elif each_scores[1] - each_scores[2] > 0.05:
                each_scores = each_scores[:2]
            else:
                each_scores = each_scores
            labels.append(all_data[name][i][:len(each_scores)])
            scores.append(each_scores)
        labels = list(chain.from_iterable(labels))
        nums_r = Counter(labels).most_common(3)
        nums_res.append(nums_r)
    
    predict_labels = []
    for value in nums_res:
        predict_labels.append([v[0] for v in value])
    all_data["three_labels"] = predict_labels
    
    final_data = {}
    for index in tqdm(range(len(ids))):
        one_ids = ids[index]
        one_text = queries[index]
        three_labels = all_data["three_labels"][ids.index(one_ids)]
        final_data[one_ids] ={"text": one_text, "labels": three_labels}
    
    return final_data
    

if __name__=="__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data_path", type=str, default="Data\\unique\\trade\\webs")
    parser.add_argument("--output_dir", type=str, default="Data\\classify")
    parser.add_argument("--labels", type=list, default=["贸易基础知识", "物流与仓储", "贸易法规与政策","财税保险","报关及单证","电商及社媒运营","市场分析风险与营销"])
    parser.add_argument("--workers", type=int, default=1)
    args = parser.parse_args()
    
    main(args)