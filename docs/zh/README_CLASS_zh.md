# 数据分类

在处理预训练数据中，需要涉及的领域就是数据分类，在清洗数据的过程中，需要进行多种分类的过滤。比如文本中有些从格式上低质量的需要过滤，更严谨的地方，我们希望预训练的时候，模型能够学习到的都是高质量的文本，所有还需要过滤内容语义上的低质量文本。

- [X] 数据格式质量分类

  数据格式质量分类模型，我们利用fasttext制作了一个简单的分类模型，用于区分数据格式质量，这里我们只是简单的区分了一下，后续可以考虑更加细致的分类。`prepare_data.py`中的 `prepare_data`函数，可以用于生成训练数据，训练数据的格式如下：

  ```
  __label__positive"+ "\t"+ text
  __label__negative"+ "\t"+ text
  ```

  其中 `__label__positive`表示数据格式质量良好，`__label__negative`表示数据格式质量不好。

  制作好训练集后，使用 `train.py`文件进行模型的训练，训练好的模型保存在 `model`文件夹中，可以使用 `test.py`文件进行简单的测试，预测的结果保存在 `result`文件夹中。
- [X] 数据内容质量分类

  数据内容质量分类模型，我们提供了三种方案供选择，一种依旧使用fasttext训练模型，一种使用bert训练模型，最后使用集成的方法进行zero shot 分类。

  当然，如果你希望分类某个垂直领域的数据，那你可能需要构建你所在垂直领域的数据。这里我们提供了一个通用的数据内容质量分类模型，可以用于分类通用的数据内容质量。我们使用WebText数据集，Meta的LLAMA使用的就是该数据集训练的内容分类，从而剔除低质量的数据。

---

## 质量分类

使用fasttext的质量分类，我们提供了两种构造数据集的方法，你可以在 `Classification`文件夹的 `prepare_dataset `与 `prepare_quality_dataset`文件用于参考，生成对应的训练文本，运行：

```
cd Classification
python train.py
```

在运行前请修改文件中得文本文件路径与需要保存的模型路径，运行完毕推理代码请参考 `inference`文件自定义修改你所需要分类推理代码。

## 内容分类

我们推荐使用zero shot集成分类的方法，运行以下：

```
cd Classification
python classify_main.py --data_path <input data path> \
	--output_dir <output data path> \
	--labels <[labels list]> \
	--workers <1>
```

其中,`data_path`代表需要分类的文本路径与文件夹路径，不能够嵌套新的文件夹；`output_dir`代表需要输出的文件夹路径，分类完成后会得到一个大体相同的文件夹，多出 `three_labels`的标签在 `meta`字段中；`labels`则表示需要将数据分到的类别名称，由于是zero shot分类，所以必须填写完整的分类标签；`workers`则代表进程数，`1`则表示使用单进程进行分类，`5`则表示使用5个进程进行分类，根据服务器最大支持来进行选择，当然也要与你的机器的内存与显卡数有关联。
