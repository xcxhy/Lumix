import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import json
import argparse
import re
import enchant
from langdetect import detect
from tqdm import tqdm
from utils import *
from itertools import chain, repeat
from concurrent import futures
import wordninja
from urllib.parse import urlparse
from unstructured.cleaners.core import bytes_string_to_string, clean_extra_whitespace, clean_non_ascii_chars, replace_unicode_quotes
from unstructured.cleaners.extract import extract_email_address, extract_ip_address, extract_ip_address_name, extract_mapi_id, extract_us_phone_number

# replace bytes string to string
def bytesstring_to_string(text):
    new_text = bytes_string_to_string(text, encoding="utf-8")
    return new_text
# delete extra whitespace
def clean_space(text):
    new_text = clean_extra_whitespace(text)
    return new_text
# delete unicode fixed
def remove_unicode_fixed(text):
    new_list = []
    for char in text:
        if (9312 <= ord(char) <= 9331) or (12881 <= ord(char) <= 12895) or (12977 <= ord(char) <= 12991):
            pass
        else:
            new_list.append(char)
    return "".join(new_list)
# delete non-ascii
def remove_non_ascii(text):
    new_text = clean_non_ascii_chars(text)
    return new_text
# delete unicode quotes
def remove_unicode_quotes(text):
    new_text = replace_unicode_quotes(text)
    return new_text
# delete other language except chinese and english
def remove_other_languge(text):
    
    return text
# delete email address
def remove_email(text):
    new_text = extract_email_address(text)
    if len(new_text) > 0:
        re_text = text
        for value in new_text:
            re_text = re_text.replace(value, "")
        return re_text
    else:
        return text
# delete IP address
def remove_ip(text):
    ip_address = extract_ip_address(text)
    ip_address_name = extract_ip_address_name(text)
    mapi_id = extract_mapi_id(text)
    # concatenate list
    new_list = list(chain.from_iterable([ip_address, ip_address_name, mapi_id]))
    if len(new_list) > 0:
        re_text= text
        for value in new_list:
            re_text = re_text.replace(value, "")
        return re_text
    else:
        return text
# delete phone number
def remove_phone(text):
    us_phone = extract_us_phone_number(text)
    zh_phone= re.findall(r'\(?0\d{2,3}[)-]?\d{7,8}',text)
    zh_phone2 = re.findall("\d{11}",text)
    phone = list(chain.from_iterable([us_phone, zh_phone, zh_phone2]))
    if len(phone) > 0:
        re_text = text
        for value in phone:
            re_text = re_text.replace(value, "")
        return re_text
    else:
        return text
# delete consecutive symbols in sentence
def remove_consecutive_symbols(sentence):
    return re.sub(r"(\W)\1+", r"\1", sentence)
# delete the string which is all numbers
def remove_all_numbers(text):
    if text.isdigit():
        return ""
    else:
        return text
# delete chinese space consecutively
def remove_space_between_chinese(text):
    return re.sub(r"([\u4e00-\u9fa5])\s+([\u4e00-\u9fa5])", r"\1\2", text)
# delete the string which is start with symbol
def remove_start_symbol(text):
    if text.startswith(("(", "[", "{", "<", "《", "【", "（", "「")):
        return text
    elif text.startswith(("\"", ":","：", ";", "」", "；", "，",",", "/", "?", "？'", "|", "\\", ">", "》", "】", "）", ")", "]", "}")):
        return text[1:]
    else:
        return text

# judge the string have shuffle words or symbols
def find_bad_sentence(text):
    pattern1 = r"[^\w\s.,?!]+|[.,?!]{2,}"
    pattern2 = r"[\p{C}\p{M}]+"
    # sentences = re.split(r"(?<!\w\.\w.)(?<![A-Z][a-z]\.)(?<=\.|\?)\s", text)
    pattern = re.compile(r"\s{1}[\w]{2}\s{1}")
    matches = re.findall(pattern, text)
    if len(matches) > 3:
        return True
    else:
        return False