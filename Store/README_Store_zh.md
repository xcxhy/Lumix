# 数据存储

对于不同得数据来源，我们通常会得到格式得数据文本，这些数据文本可能是 `json`,`txt`,`pdf`,`word`等等，我们需要将这些数据文本转换为我们需要的数据格式，然后根据固定格式存储到数据库中，以便后续的数据分析。

目前，我们支持得读取格式包括:`json`,`txt`,`pdf`,`docx`,`csv`,`xlsx`,`epub`,`ppt`.

注意：`pdf`格式读取比较复杂，我们测试了多种读取方式 `pdfplumber`,`pytesseract`,`pdfminer`,`ocrmypdf`等，由于存在单双栏以及扫描件等一系列问题，效果都不是很好，最终，我们测试最优得方式是使用 `paddleocr`库使用图片OCR的方式进行读取.

### 数据存储格式

</details>

<details><summary>点击查看</summary>

```
{
    "file_title": "Elementary_theory_of_factoring_trinomials_with_integer",
    "file_type": "pdf",
    "file_address":"Elementary_theory_of_factoring_trinomials_with_integer.pdf",
    "file_pages": 9,
    "file_language": "en",
    "file_dir": "book_en",
    "file_tokens": 6650,
    "file_id": "PLWAze3iG6So",
    "text": "This article ...",
}
```

</details>

我们为每一个文件标记唯一的id，同时统计文件的页数，语言，tokens数量，文件类型等信息，方便后续的数据分析。

### 安装依赖

```
pip install -r requirements.txt
```

### 使用方法

```
cd Store
python store.py --init_path <str> \
                --concat_path <str> \
                --save_path <str> \
                --tokenizer_path <str> \
                --file_type <list> \
                --mode <str> \
                --file_numbers <int> \
                --read_text <bool> \
                --read_info <bool> \
                --workers <int> 
```

运行上面 `store.py`则能够开始处理数据，其中参数的含义如下:
    `init_path`代表初始化文件路径，如果是第一次运行代码必须填写该路径;
    `concat_path`代表拼接文件路径; `save_path`代表存储文件路径;
    `tokenizer_path`代表分词器路径;`file_type`代表需要处理的文件类型;
    `mode`代表处理模式; `file_numbers`代表处理文件数量;
    `read_text`代表是否读取文本; `read_info`代表是否读取文件信息;
    `workers`代表多进程数量.

如果不想一次性将所有数据进行读取，可以将 `file_numbers`设置为一个较小的数，比如 `100`，这样只会读取 `100`个文件，运行 `store.sh`则可以按一定个数读取数据后保存，然后继续读取数据，直到读取完所有数据.

#### 参数设置规则

`mode`设置成 `init`则代表初始化文件，`init_path`必须填写，`concat_path`不需要填写.

`file_type`是必填写的,代表需要读取的文件类型,默认是 `["words"]`,可以输入多种类型，比如 `["words","pdf"]`.

`read_info`代表读取文件的信息，页数等，第一次运行 `read_info`必须设置为 `True`, `read_text`则代表读取文件的文本内容，设置为 `True`则会读取，否则只会读取文件信息标识.`file_numbers`默认为 `-1`，表示读取所有文件，如果你只想读取部分文件，可以设置为大于 `0`的整数，比如 `100`，则只会读取 `100`个文件.

`tokenizer_path`必须填写, 代表统计 `tokens`的字表.

`workers`代表进程数，如果你的需要读取的文件过多，你可以设置 `workers`为大于 `1`的整数，这样可以加快读取速度,默认使用`1``,单进程读取.

`store.py`打开read_info后会得到一个file_info.json文件，里面存储了文件的信息，比如文件的页数，语言，tokens数量，文件类型等信息，方便后续的数据分析;打开read_text后会得到一个file_text.json文件，里面存储了文件的文本内容,同时报错一个file_ids.json文件，里面存储了已读文件的id，方便后续的数据读取.

---

## 固定格式

为了后续更好的对数据进行完整的处理，需要对不同来源的数据进行固定位置，同时对不同来源的数据设置唯一的ID编码，方便后续文件数量扩大所带来的影响。

### 使用方法

```
cd Store
python unified_format.py --path <path> --id-path <id_path> --save-dir <save_dir>
```

在运行 `unified_format.py`之前，你需要确认自己的文件名符合标准，同时你需要自定义适合你文件的读取格式。你需要在文件中修改 `read_trade_customs`函数，从而适配你得文件，从而得到一个标准得保存格式。

我们所设置的标准的保存格式为：

<details><summary>标准格式</summary>

```
{
	"unique_id" : xx,
	"text" : xx,
	"meta" : {xx: xx, xx: xx}
}
```

在运行 `unique_format.py`为每条样本增加唯一ID后,会得到一个 `unique_ids.json`文件用于保存所有已经使用的ID，防止出现ID重复的情况与一个 `filename.json`文件用于存储新的文本文件。
