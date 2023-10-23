import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import *
from itertools import chain, repeat
from multiprocessing import Pool
from concurrent import futures


# filter messy code
def filter_messy_code_text(data):
    '''
    data: a list of text
    '''
    start = time.time()
    print('Start filtering messy code text...')
    positive_list, negative_list = fasttext_format_inference_singel(data)
    end = time.time()
    print('Filtering messy code text done, time cost: %.2f s' % (end - start))
    
    return positive_list


