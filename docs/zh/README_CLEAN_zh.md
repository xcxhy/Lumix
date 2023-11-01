# 文本清洗

&emsp;&emsp;文本清洗是数据工程的最后一步，也是相对复杂度最高的一步，清洗的过于用力会导致，好的文本被误删或者导致文本语义不通，简单的清洗可能不能提升数据的质量。

&emsp;&emsp;由于是获取训练数据的最后一步，所以需要能够做到快速动态的处理或者需要大量数据的时候提前处理保存至相应位置。我们主要从文档级别与句子级别入手进行不同细粒度的清洗。

---


### 篇章级别

- [X] 替换敏感词至固定字符或空格
- [X] 敏感词次数过多直接过滤该样本

### 句子级别

- [X] 删除连续的空格
- [X] 删除电子邮件地址
- [X] 删除IP地址与IP地址名称
- [X] 删除人名
- [X] 删除特殊符号
- [X] 分割粘连英文单词
- [X] 删除连续的符号
- [X] 删除结尾没有完整符号的句子
- [X] 替换匹配规则项

#### 匹配规则项

* [X] 中文中括号包含内容删除
* [X] 关键词

#### 使用方式

&emsp;&emsp;直接运行:

```
cd Clean
python text_clean.py --data_path <data path> \
	--part_ids_path <tradename to ids file path> \
	--ids_path <filtered ids path> \
	--output_file <save path> \
	--workers <worker number>
```

&emsp;&emsp;其中 `data_path`代表需要清洗的文件路径或文件夹路径；`part_ids_path`代表每个文件中存储的 `ID`的文件路径；`ids_path`代表过滤后的 `ID`存储文件路径；`output_file`代表保存的路径；`workers`代表单进程还是多进程的 `workers`数量。

#### 数据清洗流程

&emsp;&emsp;如何清洗数据是一个复杂的情况，需要根据具体的数据而设计，我们设计了一个基础数据清洗流程，用于简单的文本清洗。我也保留了自定义的接口在 `Focus_Clean/clean.py`中，可以根据自己的需求进行修改。

&emsp;&emsp;自定义 `clean_text_customed`函数，输入为一个字符串，输出为清洗后的字符串。中间的清洗流程可以根据自己的需求进行修改。我在 `Focus_Clean/clean.py`与 `Focus_Clean/clean_content.py`为你实现了不少清洗方式，可供参考。在定义好自定义好 `clean_text_customed`清洗函数后，在 `clean_main.py`中掉用 `clean`函数时，记得将 `use_customed`参数设置为 `True`。
