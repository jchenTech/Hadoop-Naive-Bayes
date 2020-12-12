import job.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import utils.Const;

public class Main {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("INPUT_PATH", Const.TRAIN_DATA_INPUT_PATH);
        conf.set("OUTPUT_PATH", Const.TRAIN_DATA_SEQUENCE_FILE_PATH);

        //将训练集的.txt输入文件生成一个SequenceFile
        InitSequenceFileJob initSequenceFileJob = new InitSequenceFileJob();
        ToolRunner.run(conf, initSequenceFileJob, args);

        // 根据InitSequenceFileJob输出的SequenceFile统计每个文档类别有多少个文档
        GetDocCountFromDocTypeJob getDocCountFromDocTypeJob = new GetDocCountFromDocTypeJob();
        ToolRunner.run(conf, getDocCountFromDocTypeJob, args);

        // 根据InitSequenceFileJob输出的SequenceFile计算
        // 每个文档类别中每个单词出现的次数
        GetSingleWordCountFromDocTypeJob getSingleWordCountFromDocTypeJob = new GetSingleWordCountFromDocTypeJob();
        ToolRunner.run(conf, getSingleWordCountFromDocTypeJob, args);

        // 根据GetSingleWordCountFromDocTypeJob输出的SequenceFile计算
        // 每个文档类型的总单词数用于条件概率计算
        GetTotalWordCountFromDocTypeJob getTotalWordCountFromDocTypeJob = new GetTotalWordCountFromDocTypeJob();
        ToolRunner.run(conf, getTotalWordCountFromDocTypeJob, args);


        // 运行测试集数据
        conf = new Configuration();
        conf.set("INPUT_PATH", Const.TEST_DATA_INPUT_PATH);
        conf.set("OUTPUT_PATH", Const.TEST_DATA_SEQUENCE_FILE_PATH);
        conf.set("DOC_TYPE_LIST", Const.DOC_TYPE_LIST);

        // 与训练集相同，将测试集多个文件生成一个SequenceFile
        initSequenceFileJob = new InitSequenceFileJob();
        ToolRunner.run(conf, initSequenceFileJob, args);

        // 读取之前所有任务输出的SequenceFile到内存中并在Setup中计算训练集的先验概率、条件概率
        // 读取InitSequenceFileJob生成的测试集的SequenceFile计算测试集的每个文档分成每一类的概率
        GetNaiveBayesResultJob getNaiveBayesResultJob = new GetNaiveBayesResultJob();
        ToolRunner.run(conf, getNaiveBayesResultJob, args);

        // 运行Evaluation程序，对各文档的贝叶斯分类结果进行评估，计算各文档FP、TP、FN、TN、
        // Precision、Recall、F1以及整体的宏平均、微平均。
        Evaluation evaluation = new Evaluation();
        ToolRunner.run(conf, evaluation, args);
    }
}
