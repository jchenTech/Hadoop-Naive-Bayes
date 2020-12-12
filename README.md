# 基于Hadoop的朴素贝叶斯分类(MapReduce实现)

该项目为Hadoop课程项目，基于MapReduce设计朴素贝叶斯文本分类器，并评估分类结果。具体的项目分析与报告内容见：https://github.com/jchenTech/Hadoop-Naive-Bayes/tree/main/report

以下简单介绍项目内容与运行说明：

## 项目内容

1. 用MapReduce算法实现贝叶斯分类器的训练过程，并输出训练模型；
2. 用输出的模型对测试集文档进行分类测试。测试过程可基于单机Java程序，也可以是MapReduce程序。输出每个测试文档的分类结果；
3. 利用测试文档的真实类别，计算分类模型的Precision，Recall和F1值。

## 数据集说明
我在数据集中选择了 `NBCorpus\Country` 文件夹下的 CHINA 和 CANA 作为本次实验的样本，其中 CHINA 类中包含 255 个文本，CANA 类中包含 263 个文本。按照 70% 与 30% 的比例选取训练集和测试集。表格如下：

|     | CANA | CHINA | INDIA |
|:---:|:----:|:-----:|:-----:|
| 训练集 | 177  | 170   | 227   |
| 测试集 | 87   | 86    | 99    |
| 总结  | 264  | 256   | 326   |

训练集与测试集的路径为 `data\TEST_DATA_FILE\CANA(CHINA)`和 `data\TRAIN_DATA_FILE\CANA(CHINA)` ，并将两个新的目录文件传到虚拟机master节点后上传到HDFS上。

```shell
# 进入data文件夹
cd /data

# 上传训练集
hdfs dfs -put TRAIN_DATA_FILE /

# 上传测试集
hadoop dfs -put TEST_DATA_FILE /
```
## 修改目录、全局变量配置文件
在项目文件`/src/main/java/utils/Const.class`中，根据需要修改文档目录名、HDFS的域名地址、停用词库。

> **必须修改Const.class中的DOC_TYPE_LIST变量**

该变量是记录文档种类的，每个文档种类用字符`@`分隔，例如我的分类任务是分两类，上传了CANA和CHINA两份数据，那么这个变量设置为：

```Java
 public static final String DOC_TYPE_LIST = "CANA@CHINA";
```

## 打成JAR包并运行

在IDEA中通过maven中的package将程序打成JAR包，打包完成后，将JAR包上传到master节点中，进入到JAR包目录，运行命令 `hadoop jar jar_file_path main_class_name` 执行JAR包，如：

```shell
hadoop jar Main.jar Main
```




