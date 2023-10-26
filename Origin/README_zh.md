# 固定格式

固定主要是为了保持数据存储格式的一致，以及为每个文本添加唯一的ID。


<details><summary>固定格式 点击产看格式</summary>

```
{
    "unique_id": "Pd41d1oEN5nR",
    "text": "亚太国际物流：连接缅甸与周边国家的全方位物流解决方案",
    "meta": {
            "timestamp": 1689911100.0,
            "url": "https://www.bilibili.com/read/cv25177531?from=search",
            "language": "en",
            "source": "c4",
            "category": "国际物流"
            }
}
```

`</details>`

## 使用方法

直接运行以下代码：

```
cd Origin
python unified_format.py --path <file path or files dir> \
	-- id_path <store ids file> \
	-- save_dir <save new format file path>
```

`path`是待处理的数据的路径，可以是单个文件路径也可以是多个文件的文件夹路径；`  id_path`是需要存储唯一id的文件路径，如果你是第一次运行会新建文件，如果已有ID文件,则会增量增加ID；`  save_dir`则是添加ID与固定格式后的数据的新路径。

当然为了保证ID与数据的准确性，我们也提供反向利用ID文本文件，更新ids文件：

运行以下代码：

```
python confirm_ids.py --dir <unique path>
	--save_path <all ids file path>
	--files_path <each file store ids path>
```

`dir`代表储存数据的路径，是能包括所有文本数据的目录地址；`save_path`代表存储所有文本里的ids的文件地址；`files_path`则是代表每一个文件里存储的ids，方便查找。
