# 数据清洗

清洗数据部分，我们主要从以下几个方面进行了数据清洗：
- 文本去重
  - [x] MinHash去重
  - [x] SimHash去重
  - [x] Fuzzy模糊去重

- 文本去噪
    </details>

    <details><summary>篇章级别</summary>

        - [x] 过滤少于固定字数得文章
        - [x] 过滤敏感词超过三个以上的文章

    </details>

    </details>

    <details><summary>段落级别</summary>

        - [x] 删除额外得空格
        - [x] 替换bytes string成string
        - [x] 删除非ascii字符
        - [x] 删除unicode quotes字符
        - [x] 删除英文与中文以外的字符
        - [x] 删除电子邮件地址
        - [x] 删除IP地址与IP地址名称
        - [x] 删除电话号码
        - [ ] 删除人名
        - [ ] 翻译文本
        - [x] 删除特殊匹配项
        - [x] 删除乱码
        - [ ] 删除主要由大写字符组成
        - [ ] 句子由纯数字组成
        - [ ] 句子去重

    </details>

#### 数据清洗流程

如何清洗数据是一个复杂的情况，需要根据具体的数据而设计，我们设计了一个基础数据清洗流程，用于简单的文本清洗。我也保留了自定义的接口在`Clean/clean.py`中，可以根据自己的需求进行修改。

自定义`clean_text_customed`函数，输入为一个字符串，输出为清洗后的字符串。中间的清洗流程可以根据自己的需求进行修改。我在`Clean/clean.py`与`Clean/clean_content.py`为你实现了不少清洗方式，可供参考。在定义好自定义好`clean_text_customed`清洗函数后，在`clean_main.py`中掉用`clean`函数时，记得将`use_customed`参数设置为`True`。


