# 文档去重

**&emsp;&emsp;文档去重**是在针对预训练语料处理工作中非常重要的一个步骤，重复的文档在预训练过程中会降低模型的性能。**文档去重**的方法其实有很多，比如 **模糊去重** ， **SimHash** ， **MinHash** ， **KSentence** ，**KShingle**等算法去重，通过**计算计算最长子串去重**也同样适用。相关算法的定义与推理详情可以观看该[博客](https://zhuanlan.zhihu.com/p/43640234)。

&emsp;&emsp;我们直接给出多种去重算法的对比结论：

| 算法      | 优缺点                                                                                                                          |
| --------- | ------------------------------------------------------------------------------------------------------------------------------- |
| KShingle  | Jaccard similarity不适合较长的文本，文档的特征集合比较大，会遇到时间和空间的双重瓶颈。                                          |
| MinHash   | MinHash利用hash加随机的方式减少文档特征集合的大小，有损但是近似Jaccard similarity的估计，同时与LSH算法结合能够大大提高效率。    |
| SimHash   | SimHash文本较长的情况，准确率较高，针对短文本的准确率不能保证。                                                                 |
| KSentence | KSentence准确率较高，召回率低于MinHash与SimHash，算法实现简单，效率高；同时也可以并行化，但是对抄袭后部分修改的文本识别度较低。 |

&emsp;&emsp;这里我们主推 `MinHash`，也是参考**LLaMA**与**Falcon**的操作，同时我们实现了 `SimHash`与 `MinHash`的组合去重，能够得到更好的效果，但是带来的牺牲是时间上的成本。

---

    这里我们为你实现了MinHash算法与MinHash-SimHash结合算法，我们推荐使用Minhash去重，这是目前大模型常用的去重算法，同时当前它已经支持LSH加速与多线程加速。

## 开始去重

**
    MinHash**的主要算法在 `min_hash_lsh.py`中实现，在运行 `unique_format.py`为每条样本增加唯一ID后,会得到一个 `unique_ids.json`文件用于保存所有已经使用的ID，防止出现ID重复的情况与一个 `filename.json`文件用于存储新的文件。我们只需要运行以下代码就能够进行文件中文本的去重：

```
cd Deduplication
python main_deduplicated.py --data_path <file path or file dir> \
	--deduplicate_mode <store|search|multi_search> \
	--workers <int> \
	--deduplicated_ids_dir <deduplicated and duplicated ids dir> \
	--hashindex_dir <minhash index dir> \
	--name <name>

```

    上述的几个变量是必须要填写的，否则程序会报错。`data_path`表示文本文件的地址，可以是单个文件的地址也可以是多个文件组成的文件夹地址，如果是文件夹需要确认文件夹中的文件不存在格式错误，同时文件夹内所有文档读取不会 `out of memory`；`deduplicate_mode`表示去重所选择的模式，有 `["store", "search", "multi_search"]`三种模式可选，第一次运行请选择 `store`模式，保存基础的 `minhash index`在 `hash_index`文件夹中，`store`模式使用自我去重的方式，而 `search`模式则是将文件与已有的 `minhash index`进行扫描去重，`multi_search`模式则是防止基础 `minhash index`文件太大，分块读取并去重，如果你的文件很大同时你的设备内存较好，建议使用该模式，同时将 `name`参数进行设置；`deduplicated_ids_dir`则是保存去重后的文本id与重复的文本id两个文件的文件夹路径；`hashindex_dir`则是保存 `hash index`索引的文件夹路径；`workers`默认是4，如果你的CPU核心与内存充足你可以设置的更高一些。
