package job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.Const;

import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;

/**
 * 首先在setup中计算训练集的先验概率、条件概率，并通过MapReduce任务计算测试集的每个文档分成每一类的概率
 * 读取InitSequenceFileJob生成的测试集的SequenceFile计算测试集的每个文档分成每一类的概率
 */
public class GetNaiveBayesResultJob extends Configured implements Tool {

    public static final Logger log = LoggerFactory.getLogger(GetNaiveBayesResultJob.class);

    // 文档种类列表
    private static String[] docTypeList;

    // 每个类别中每个单词出现的次数
    private static Map<String, Integer> eachWordCountInDocTypeMap = new HashMap<>();

    // 每个类别中所有单词出现的次数
    private static Map<String, Integer> allWordCountInDocTypeMap = new HashMap<>();

    // 每个文档Ci的先验概率P(Ci)
    private static Map<String, Double> docTypePriorProbabilityMap = new HashMap<>();

     // 每个单词Wi的条件概率P(Wi|Ci)
    private static Map<String, Double> wordConditionalProbabilityMap = new HashMap<>();

    // 每个文档的预测结果
    private static Map<String, String> docPredictResultMap = new HashMap<>();

    // 单词的正则表达式
    private static final Pattern ENGLISH_WORD_REGEX = Pattern.compile("^[A-Za-z]{2,}$");


    public static class GetNaiveBayesResultMapper extends Mapper<Text, BytesWritable, Text, Text> {

        // 测试集中单词的条件概率
        Text conditionalProbabilityValue = new Text();

        /*
         * 读取之前所有任务输出的SequenceFile到内存中并在Setup中计算训练集的先验概率、条件概率
         */
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
//            System.out.println("start GetNaiveBayesResultMapper's setup()");
            Configuration conf = context.getConfiguration();
            Path getDocCountFromDocTypePath = new Path(Const.GET_DOC_COUNT_FROM_DOC_TYPE_JOB_OUTPUT_PATH + Const.HADOOP_DEFAULT_OUTPUT_FILE_NAME);
            Path getSingleWordCountFromDocType = new Path(Const.GET_SINGLE_WORD_COUNT_FROM_DOC_TYPE_JOB_OUTPUT_PATH + Const.HADOOP_DEFAULT_OUTPUT_FILE_NAME);
            Path getTotalWordCountFromDocType = new Path(Const.GET_TOTAL_WORD_COUNT_FROM_DOC_TYPE_JOB_OUTPUT_PATH + Const.HADOOP_DEFAULT_OUTPUT_FILE_NAME);

            conf.set("INPUT_PATH", Const.TEST_DATA_INPUT_PATH);
            conf.set("OUTPUT_PATH", Const.TEST_DATA_SEQUENCE_FILE_PATH);
            conf.set("DOC_TYPE_LIST", Const.DOC_TYPE_LIST);

            docTypeList = conf.get("DOC_TYPE_LIST").split("@");

            FileSystem fs = FileSystem.get(conf);
            // 读取sequence_file
            SequenceFile.Reader reader = null;
            double totalDocCount = 0;
            Map<String, Integer> eachDocTypeDocCountMap = new HashMap<String, Integer>(10);
            try {
                //从sequence_file中读取每个文档类型中的总单词数
                SequenceFile.Reader.Option option = SequenceFile.Reader.file(getDocCountFromDocTypePath);
                reader = new SequenceFile.Reader(conf, option);
                Text key = new Text(); // key: CANA
                IntWritable value = new IntWritable(); // value: 300
                while (reader.next(key, value)) {
                    eachDocTypeDocCountMap.put(key.toString(), Integer.parseInt(value.toString()));
                    totalDocCount += value.get();
                }
            } catch (Exception ex) {
                log.error(ex.getMessage());
            } finally {
                // 确保发生异常时关闭reader
                IOUtils.closeStream(reader);
            }

            // 计算文档Ci的先验概率：P(Ci)=类型Ci的文档数/总文档数
            double finalTotalDocCount = totalDocCount;
            eachDocTypeDocCountMap.forEach((docTypeName, docCount) -> {
                double priorProbability = docCount / finalTotalDocCount;
                docTypePriorProbabilityMap.put(docTypeName, priorProbability);
                System.out.println("文档类型 " + docTypeName + " 的先验概率P(Ci)=" + priorProbability);
            });

            // 取出sequence_file中存储的类别中每个单词出现的次数 存储到Map中 形式为：CANA@hello 13
            try {
                SequenceFile.Reader.Option option = SequenceFile.Reader.file(getSingleWordCountFromDocType);
                reader = new SequenceFile.Reader(conf, option);
                Text key = new Text(); // key: CANA@hello
                IntWritable value = new IntWritable(); // value: 13
                while (reader.next(key, value)) {
                    eachWordCountInDocTypeMap.put(key.toString(), value.get());
                }
            } catch (Exception ex) {
                log.error(ex.getMessage());
            } finally { // 确保发生异常时关闭reader
                IOUtils.closeStream(reader);
            }

            // 取出sequence_file中存储的每个类别中的所有单词出现的总次数
            try {
                SequenceFile.Reader.Option option = SequenceFile.Reader.file(getTotalWordCountFromDocType);
                reader = new SequenceFile.Reader(conf, option);
                Text key = new Text(); // key: CANA
                IntWritable value = new IntWritable(); // value: 184032
                while (reader.next(key, value)) {
                    allWordCountInDocTypeMap.put(key.toString(), value.get());
                }
            } catch (Exception ex) {
                log.error(ex.getMessage());
            } finally { // 确保发生异常时关闭reader
                IOUtils.closeStream(reader);
            }

            // 计算每个单词的条件概率
            eachWordCountInDocTypeMap.forEach((key, value) -> {
                String docType = key.split("@")[0];
                String word = key.split("@")[1];
                double probability = (value.doubleValue() + 1.0) / allWordCountInDocTypeMap.get(docType).doubleValue();
                wordConditionalProbabilityMap.put(key, probability);
            });
        }

        /*
         * 重写map函数，输出为<文档类型@文件名，文档类型@概率>
         */
        @Override
        protected void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
            // key: CANA@487557newsML.txt
            // value: 487557newsML.txt的文件内容

            //计算文档d为类别Ci的条件概率：P(d|Ci)= ∏P(Wi|Ci)
            // 将sequence_file中的bytes读成字符串
            String content = new String(value.getBytes());
            String[] wordArray = content.split("\\s+");
            for (String docTypeName : docTypeList) {
                double conditionalProbability = 0;
                for (String word : wordArray) {
                    if (ENGLISH_WORD_REGEX.matcher(word).find() && !Const.STOP_WORDS_LIST.contains(word)) {
                        String wordKey = docTypeName + "@" + word;
                        if (wordConditionalProbabilityMap.containsKey(wordKey)) {
                            conditionalProbability += Math.log10((wordConditionalProbabilityMap.get(wordKey)));
                        } else {
                            // 如果测试集出现了训练集中没有出现过的单词，那么就把该单词在类型为Ci的文档中出现的次数设置为1
                            conditionalProbability += Math.log10(1.0 / allWordCountInDocTypeMap.get(docTypeName).doubleValue());
                        }
                    } else {
                        log.debug("过滤无用词：" + word);
                    }
                }
                // 再加上文档Ci的条件概率
                conditionalProbability += Math.log10(docTypePriorProbabilityMap.get(docTypeName));
                this.conditionalProbabilityValue.set(docTypeName + "@" + conditionalProbability);
                context.write(key, conditionalProbabilityValue);
            }
        }
    }

    public static class GetNaiveBayesResultReducer extends Reducer<Text, Text, Text, Text> {

        // 测试集中文档被分为Ci类的概率
        Text docTypeForecastResult = new Text();

        /*
         * 重写reduce函数，输入为<文档类型@文件名，文档类型@概率>，输出为<文档类型@文件名，文档类型@最大概率>
         */
        @Override
        // 计算文档d是哪一类
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // key: CANA@487557newsML.txt
            // value : CANA@-334.2343 / CHINA@-484.49404

            // 最大概率默认负无穷
            double maxProbability = Double.NEGATIVE_INFINITY;
            String forecastDocType = "";
            for (Text value : values) {
                double forecastProbability = Double.parseDouble(value.toString().split("@")[1]);
                if (forecastProbability > maxProbability) {
                    maxProbability = forecastProbability;
                    forecastDocType = value.toString().split("@")[0];
                }
            }
            this.docTypeForecastResult.set(forecastDocType + "@" + maxProbability);
            context.write(key, docTypeForecastResult);
//            System.out.println(key.toString() + " 预测分类为： " + forecastDocType + " ，预测概率为：" + maxProbability);
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        System.out.println("开始对 GetNaiveBayesResultJob 进行配置");

        Configuration conf = new Configuration();

        // 如果输出目录存在，则先删除输出目录
        Path outputPath = new Path(Const.GET_NAIVE_BAYES_RESULT_JOB_OUTPUT_PATH);
        FileSystem fs = outputPath.getFileSystem(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        Job job = Job.getInstance(conf, "GetNaiveBayesResultJob");

        job.setJarByClass(GetNaiveBayesResultJob.class);
        job.setMapperClass(GetNaiveBayesResultJob.GetNaiveBayesResultMapper.class);
        job.setCombinerClass(GetNaiveBayesResultJob.GetNaiveBayesResultReducer.class);
        job.setReducerClass(GetNaiveBayesResultJob.GetNaiveBayesResultReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(Const.TEST_DATA_SEQUENCE_FILE_PATH));
        FileOutputFormat.setOutputPath(job, new Path(Const.GET_NAIVE_BAYES_RESULT_JOB_OUTPUT_PATH));

        System.out.println("完成配置，开始执行 GetNaiveBayesResultJob");
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new GetNaiveBayesResultJob(), args);
        System.out.println("GetNaiveBayesResultJob 运行结束");
        System.exit(res);
    }
}
