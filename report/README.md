## 1 Project内容

1. 用MapReduce算法实现贝叶斯分类器的训练过程，并输出训练模型；
2. 用输出的模型对测试集文档进行分类测试。测试过程可基于单机Java程序，也可以是MapReduce程序。输出每个测试文档的分类结果；
3. 利用测试文档的真实类别，计算分类模型的Precision，Recall和F1值。

 

## 2 贝叶斯分类器理论介绍

### 2.1 朴素贝叶斯方法介绍

问题：给定一个类标签集合 $C = {c_1,c_2, …, c_j}$ 以及一个文档d，给文档d分配一个最合适的类别标签

基本思想：对于类标签集合 C 中的每个类标签 $c_i (i = 1, …, j),$ 计算条件概率 $p (ci |d)$，使条件概率 $p (ci |d)$ 最大的类别作为文档d最终的类别。

* 基于概率的思想，给定一个文档d，看该文档属于哪个类别的可能性最大，就认为该文档属于哪个类别。
* 类似于赌博游戏，要你猜硬币的正、反面，猜对了有奖励。假如我告诉你出现正面的几率是60%，现负面的几率是40%，你会怎么选择？当然会选择正面，因为这样你有更高的几率赢钱。

因此Naïve Bayes是一个基于概率的分类器。Naïve Bayes是一个简单、速度快的分类器，效果也非常好。经常作为分类器性能比较的基准（Base line）。

### 2.2 Bayes公式

上面我们提到了通过计算每个类的条件概率来选择最大的概率的类别作为文档d的最终类别，那么我们该如何计算条件概率呢。这里我们要利用Bayes公式：
$$
p({c_i}|d) = \frac{{p(d|{c_i})p({c_i})}}{{p(d)}}
$$
其中：

* $p({c_i}|d)$ 为后验概率或条件概率
* $p({c_i})$ 为先验概率
* $p(d|{c_i})$ 为似然概率
* $p(d)$ 为证据

**Bayes公式的意义：**

1、当观察到 evidence $p(d)$ 时，后验概率 $p (ci|d)$ 取决于似然概率 $p (d|ci )$ 和先验概率 $p(ci)$。因为当evidence $p(d)$ 已知时，$p(d)$ 成为常量，Bayes公式变成：
$$
p({c_i}|d) = \frac{{p(d|{c_i})p({c_i})}}{{p(d)}} \propto p(d|{c_i})p({c_i})
$$
2、当先验概率 $p (c1)=p (c2)=…=p (cj) $时，公式变为：
$$
p({c_i}|d) \propto p(d|{c_i})
$$
这时给定文档d，该文档属于类别 $c_i$ 的概率 $p(c_i |d )$ 取决于似然概率 $p (d|c_i)$

* $p (d|c_i)$ 的涵义：给定文档类别 $c_i$，由类别 $c_i$ 产生文档d的可能性（likelihood）。
* 如果类别 $c_i$ 产生文档d的可能性 $p(d|ci)$ 最大，则文档d属于类别 $c_i$ 的概率 $p(c_i |d)$ 最大。这叫最大似然估计（Maximum Likelihood Estimation，MLE）。

### 2.3 朴素贝叶斯参数推导

现在再回到Naïve Bayes分类器
$$
p({c_i}|d) = \frac{{p(d|{c_i})p({c_i})}}{{p(d)}} \propto p(d|{c_i})p({c_i})
$$
对于类标签集合C中的每个类标签 $c_i (i = 1, …, j)$ , 计算条件概率 $p(c_i |d)$，使条件概率 $p(c_i |d)$最大的类别作为文档d最终的类别。
$$
{c_d} = \mathop {\arg \max }\limits_{{c_i} \in C} p({c_i}|d) = \mathop {\arg \max }\limits_{{c_i} \in C} p(d|{c_i})p({c_i})
$$
根据Bayes公式， $c_d$ =使得 $p(d | c_i) p (c_i)$ 值最大的类型。剩下的问题是如何得到 $p(d | c_i) p (c_i)$ ？对于Naïve Bayes，用训练集对机器进行训练就是为了算出这两个参数，训练的过程就是参数估计的过程。

**参数估计的过程为：**

假设类别标签集合 $C = {c1,c2, …, cj}$ 。假设训练集D包含N个文档，其中每个文档都被标上了类别标签。

1、首先估计先验概率 $p (c_i) (i=1, …, j)$
$$
p({c_i}) = \frac{{类型为{c_i}的文档个数}}{训练集中文档总数{N}}
$$
2、估计似然概率 $p(d | c_i)(i=1,...,j)$

为了估计 $p(d | c_i)$，需要一个假设：Term独立性假设，即文档中每个term的出现是彼此独立的 。基于这个假设，似然概率 $p(d | c_i)$ 的估计方法如下，假设文档d包含nd个term：t1, t2, …, tnd：
$$
p(d|{c_i}) = p({t_1},{t_2},...,{t_{{n_d}}}|{c_i}) = p({t_1}|c)p({t_2}|c)...p({t_{{n_d}}}|c) = \prod\limits_{1 \le k \le {n_d}} {p({t_k}|{c_i})} 
$$
因此，估计 $p(d | c_i)$ 就需要估计 $p(t_k|c_i)$ ：
$$
p({t_k}|{c_i}) = \frac{{{t_k}在类型为{c_i}的文档中出现的次数}}{{在类型为{c_i}的文档中出现的term的总数}}
$$

### 2.4 朴素贝叶斯分类器总结

对于类标签集合C中的每个类标签 $c_i (i = 1, …, j)$, 计算条件概率 $p(c_i |d)$，使条件概率 $p(c_i |d)$ 最大的类别作为文档d最终的类别，即：
$$
{c_d} = \mathop {\arg \max }\limits_{{c_i} \in C} p({c_i}|d) = \mathop {\arg \max }\limits_{{c_i} \in C} p(d|{c_i})p({c_i}) = \mathop {\arg \max }\limits_{{c_i} \in C} \prod\limits_{1 \le k \le {n_d}} {p({t_k}|{c_i})} p({c_i})
$$
其中参数 $p(ci) (i=1, …, j)$ 通过训练集来估计
$$
p({c_i}) = \frac{{类型为{c_i}的文档个数}}{训练集中文档总数{N}}
$$
参数 $p(t_k|c_i)$ 通过训练集来估计
$$
p({t_k}|{c_i}) = \frac{{{t_k}在类型为{c_i}的文档中出现的次数}}{{在类型为{c_i}的文档中出现的term的总数}}
$$

## 3 贝叶斯分类器训练的MapReduce算法设计

基于MapReduce的朴素贝叶斯文档分类器算法主要包含`InitSequenceFileJob.java`、`InitSequenceFileInputFormat.java`、`GetDocCountFromDocTypeJob.java`、`GetSingleWordCountFromDocTypeJob.java`、`GetTotalWordCountFromDocTypeJob.java`、`GetNaiveBayesResultJob.java`和`Evaluation.java`7个程序组成，其中前面为MapReduce程序，Evaluation是单机程序，用来评估分类结果。

### 3.1 InitSequenceFileJob

`InitSequenceFileJob`中将.txt格式的输入文件转换成SequenceFile，其中的Map后的Key是“文档类型@文件名”，Value为输入的txt文件的内容，通过重写`FileInputFormat`和`RecordReader` 将输入的文件内容处理成byte数组进行进行存储。Reduce不做任何操作，因此所有的.txt文件输出为一个序列文件，里面的Key-Value对即为 `<文档类型@文件名，文档内容 >`。

![InitSequenceFileJob中MapReduce程序的Dataflow图](https://gitee.com/jchenTech/images/raw/master/img/20201206225626.png)



### 3.2 GetDocCountFromDocTypeJob

`GetDocCountFromDocTypeJob.java` 中根据 `InitSequenceFileJob` 输出的sequence_file统计每个DocType有多少个文档。具体来说：

1. Map输入为Key-Value对为 `<文档类型@文件名，文档内容 >` 的SequenceFile，Map之后的key为文档类型，如：CANA或CHINA，value为1，即每个文档对应一个键值对 `<文档类型，1>` 
2. Map输出的键值对传给Combine，将相同key的键值对中的value合并为一个数组，此时的key-value对为：`<文档类型，[1,1,...]>` 
3. Combine的输出交给Reducer，在Reduce中对value数组进行求和，这样就得到了每个文档类型如：CANA和CHINA的文档总数，计算出来的文档总数将用于训练后续的先验概率。

![GetDocCountFromDocTypeJob中MapReduce的DataFlow图](https://gitee.com/jchenTech/images/raw/master/img/20201206225627.png)



### 3.3 GetSingleWordCountFromDocTypeJob

`GetSingleWordCountFromDocTypeJob.java` 程序根据`InitSequenceFileJob`输出的SequenceFile统计每个单词在每个文档类别中出现的次数。具体来说：

1. Map的输入为Key-Value对为 `<文档类型@文件名，文档内容 >` 的SequenceFile，Map之后的key为文档类型@单词，如：CANA@change，value为1，即每个文档中的单个单词对应一个键值对 `<文档类型@单词，1>` 
2. Map输出的键值对传给Combine，将相同key的键值对中的value合并为一个数组，此时的key-value对为：`<文档类型@单词，[1,1,...]>` 
3. Combine的输出交给Reducer，在Reduce中对value数组进行求和，这样就得到了每个文档类型中所有单个单词的数量，如：change在CANA类别中的总数，计算出来的单词总数将用于训练后续的条件概率，此时输出的key-value对为：`<文档类型@单词，单词总数>` 。

![GetSingleWordCountFromDocTypeJob中的MapReduce的DataFlow图](https://gitee.com/jchenTech/images/raw/master/img/20201206225628.png)



### 3.4 GetTotalWordCountFromDocTypeJob

`GetTotalWordCountFromDocTypeJob.java` 程序根据 `GetSingleWordCountFromDocTypeJob` 输出的sequence_file统计每个文档类别的总单词数。具体来说：

1. Map的输入为Key-Value对为 `<文档类型@单词，单词数>` 的SequenceFile，Map之后的key为文档类型，如：CANA，value为每个单词在该文档类别中的总数，即每个文档中的单个单词对应一个键值对 `<文档类型，某个单词总数>` 
2. Map输出的键值对传给Combine，将相同key的键值对中的value合并为一个数组，此时的key-value对为：`<文档类型，[Count1,Count2,...]>` 
3. Combine的输出交给Reducer，在Reduce中对value数组进行求和，这样就得到了每个文档类型中所有单词的总数量，如：CANA类型中的单词总数，计算出来的单词总数将用于训练后续的条件概率，此时输出的key-value对为：`<文档类型，单词总数>` 。

![GetTotalWordCountFromDocTypeJob中的MapReduce的DataFlow图](https://gitee.com/jchenTech/images/raw/master/img/20201206225629.png)



### 3.5 GetNaiveBayesResultJob

`GetNaiveBayesResultJob.java` 程序根据 `InitSequenceFileJob` 输出的sequence_file统计每个文档类别的总单词数。具体来说：

1. 在Setup中通过前面计算出的文档总数，单词总数，单个单词在文档中出现总数，计算训练集的先验概率、条件概率。
2. Map的输入为Key-Value对为 `<文档类型@文件名，文件内容>` 的SequenceFile，Map之后的key为文档类型@文件名，如：CANA@477888newsML.txt，value为该文档属于每个类别的概率，即每个文档对应一个键值对 `<文档类型@文件名，每个类别对应的概率>` 。
3. Map的输出交给Reducer，在Reduce中计算属于每个文档概率的最大值作为value，这样就得到了每个文档属于最大概率的类别，此时输出的key-value对为：`<文档类型@文件名，文档类型@最大条件概率>` 。

![NaiveBayes](https://gitee.com/jchenTech/images/raw/master/img/20201206225630.png)

### 3.6 Evaluation

`Evaluation.java` 程序对各文档的贝叶斯分类结果进行评估，计算各文档FP、TP、FN、TN、Precision、Recall、F1以及整体的宏平均、微平均。注：该程序为单机程序而非MapReduce程序。



## 4 源代码清单

## 5 数据集说明

我在数据集中选择了 `Country` 文件夹下的 CHINA 和 CANA 作为本次实验的样本，其中 CHINA 类中包含 255 个文本，CANA 类中包含 263 个文本。按照 70% 与 30% 的比例选取训练集和测试集。表格如下：

|          | CHINA | CANA |
| -------- | ----- | ---- |
| 文档总数 | 255   | 263  |
| 训练集数 | 178   | 184  |
| 测试集数 | 77    | 79   |

## 6 程序运行说明

该项目一共要运行6个Map和Reduce任务，具体如下

1、InitSequenceFileJob

两个InitSequenceFileJob分别是对测试机和训练集文件进行序列化操作，将.txt文件输出为SequenceFile。

![训练集的InitSequenceFileJob任务运行截图](https://gitee.com/jchenTech/images/raw/master/img/20201206225631.png)



2、GetDocCountFromDocTypeJob

GetDocCountFromDocTypeJob有1个Map任务和1个Reduce任务，根据InitSequenceFileJob输出的sequence_file经过Map和Reduce后统计每个DocType有多少个文档。

![GetDocCountFromDocTypeJob任务运行截图](https://gitee.com/jchenTech/images/raw/master/img/20201206225632.png)



3、GetSingleWordCountFromDocTypeJob

GetSingleWordCountFromDocTypeJob有1个Map任务和1个Reduce任务，根据InitSequenceFileJob输出的sequence_file统计每个单词在每个文档类别中出现的次数。

![GetSingleWordCountFromTypeJob任务运行截图](https://gitee.com/jchenTech/images/raw/master/img/20201206225633.png)



4、GetTotalWordCountFromDocTypeJob

GetTotalWordCountFromDocTypeJob有1个Map任务和1个Reduce任务，根据GetSingleWordCountFromDocTypeJob输出的sequence_file统计每个文档类别的总单词数。

![GetTotalWordCountFromDocTypeJob任务运行截图](https://gitee.com/jchenTech/images/raw/master/img/20201206225634.png)



5、GetNaiveBayesResultJob

GetNaiveBayesResultJob有1个Map任务和1个Reduce任务，读取InitSequenceFileJob生成的测试集的sequence_file计算测试集的每个文档分成每一类的概率。

![GetNaiveBayesResultJob任务运行截图](https://gitee.com/jchenTech/images/raw/master/img/20201206225635.png)



6、Evaluation

Evaluation程序为单机程序，因此没有Map和Reduce任务，该程序对各文档的贝叶斯分类结果进行评估，计算各文档FP、TP、FN、TN、Precision、Recall、F1以及整体的宏平均、微平均。

![Evaluation程序运行截图](https://gitee.com/jchenTech/images/raw/master/img/20201206225636.png)



7、Web页面的作业监控截图

![WEB界面作业监控截图](https://gitee.com/jchenTech/images/raw/master/img/20201206225637.png)

![WEB界面任务计算结果](https://gitee.com/jchenTech/images/raw/master/img/20201206225638.png)



## 7 实验结果分析

将上述的实验结果进行整理如下：

| CANA            | Yes(Ground Truth) | No(Ground Truth) |
| --------------- | ----------------- | ---------------- |
| Yes(Classified) | 79                | 40               |
| No(Classified)  | 0                 | 37               |

| CHINA           | Yes(Ground Truth) | No(Ground Truth) |
| --------------- | ----------------- | ---------------- |
| Yes(Classified) | 37                | 0                |
| No(Classified)  | 40                | 79               |

其中CANA的准确率为0.6638655，召回率为1，F1值为0.797979，CHINA的准确率为1，召回率为0.48051948，F1值为0.649122807。

微平均的计算结果为：

* Precision： 0.7435897435897436
* Recall： 0.7435897435897436
* F1： 0.7435897435897437

宏平均的计算结果为：

* Precision： 0.8319327731092436
* Recall： 0.7402597402597403
* F1： 0.723551302498671