# 中文文档过滤

文档去重后，我们需要对文档进行一部分得过滤，过滤低质量得文档。这里得低质量包括几种情况：

1. 长度过短：少于50个tokens
2. 文本开头与结尾出现广告
3. 文章中出现乱码
4. 文档中语义不通

这类数据过滤后，再通过一定得清洗能够再进行使用。

---

### 过滤长度过短

---

### 过滤广告

---

### 过滤乱码

---

### 过滤语义不通

## 使用方式

运行以下代码

```
cd Filter
python text_filter.py --data_path <data path> \
	--part_ids_path <filename ids path> \
	--ids_path <deduplicated ids path> \
	--output_file <save filter ids path> \
	--tokenizer_path <tokenizer path> \
	--workers <int>
```

其中，`data_path`代表待过滤文件的路径，最好选用文件夹路径，程序会对文件夹内的文件一个一个过滤；`part_ids_path`则需要给与之前流程中所保存的 `文件名-ids`的文件地址，方便程序读取对应的文件 `ids`；`ids_path`代表去重后的 `ids文件路径`；`output_file`则表示过滤后的总ids文件需要存储路径；`tokenizer_path`代表需要所使用的模型地址；`workers`则依旧是单进程还是多进程的开关。
