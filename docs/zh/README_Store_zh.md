# 数据存储

## 固定格式

&emsp;&emsp;为了后续更好的对数据进行完整的处理，需要对不同来源的数据进行固定位置，同时对不同来源的数据设置唯一的ID编码，方便后续文件数量扩大所带来的查找等操作的影响。

### 使用方法

```
cd Store
python unified_format.py --path <file path or files dir> \
	-- id_path <store ids file> \
	-- save_dir <save new format file path>
```

&emsp;&emsp;`path`是待处理的数据的路径，可以是单个文件路径也可以是多个文件的文件夹路径；`  id_path`是需要存储唯一ID的文件路径，如果你是第一次运行会新建文件，如果已有ID文件,则会增量增加ID；`  save_dir`则是添加ID与固定格式后的数据的新路径。
&emsp;&emsp;在运行 `unified_format.py`之前，你需要确认自己的文件名符合标准，同时你需要自定义适合你文件的读取格式。你需要在文件中修改 `read_trade_customs`函数，从而适配你得文件，从而得到一个标准得保存格式。

我们所设置的标准的保存格式为：

<details><summary>标准格式</summary>

```
{
	"unique_id" : xx,
	"text" : xx,
	"meta" : {xx: xx, xx: xx}
}
```

&emsp;&emsp;在运行 `unique_format.py`为每条样本增加唯一ID后,会得到一个 `unique_ids.json`文件用于保存所有已经使用的ID，防止出现ID重复的情况与一个 `filename.json`文件用于存储新的文本文件。

### 反向确认

&emsp;&emsp;当然为了保证ID与数据的准确性，我们也提供反向利用ID文本文件，更新ids文件：

&emsp;&emsp;运行以下代码：

```
python confirm_ids.py --dir <unique path>
	--save_path <all ids file path>
	--files_path <each file store ids path>
```

&emsp;&emsp;`dir`代表储存数据的路径，是能包括所有文本数据的目录地址；`save_path`代表存储所有文本里的ids的文件地址；`files_path`则是代表每一个文件里存储的ID，方便查找。
