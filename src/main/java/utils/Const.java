package utils;

import java.util.*;


public class Const {
    private static String[] stopWordsArray = {"A", "a", "the", "an", "in",
            "on", "and", "The", "As", "as", "AND"};

    // 目录前缀
    public static final String BASE_PATH = "hdfs://master:8020";

    // 训练集目录
    public static final String TRAIN_DATA_INPUT_PATH = BASE_PATH + "/TRAIN_DATA_FILE";

    // 测试集目录
    public static final String TEST_DATA_INPUT_PATH = BASE_PATH + "/TEST_DATA_FILE";

    // 生成训练集sequence_file的输出目录
    public static final String TRAIN_DATA_SEQUENCE_FILE_PATH = BASE_PATH + "/TRAIN_DATA_SEQUENCE_FILE";

    // 生成测试集sequence_file的输出目录
    public static final String TEST_DATA_SEQUENCE_FILE_PATH = BASE_PATH + "/TEST_DATA_SEQUENCE_FILE";

    // 获取文档数的输出目录
    public static final String GET_DOC_COUNT_FROM_DOC_TYPE_JOB_OUTPUT_PATH = BASE_PATH + "/GET_DOC_COUNT_JOB_OUTPUT";

    // 获取每个文档类型中每个单词出现的次数的输出目录
    public static final String GET_SINGLE_WORD_COUNT_FROM_DOC_TYPE_JOB_OUTPUT_PATH = BASE_PATH + "/GET_SINGLE_WORD_COUNT_FROM_DOC_TYPE_JOB_OUTPUT";

    // 获取每个文档种类的总词数的输出目录
    public static final String GET_TOTAL_WORD_COUNT_FROM_DOC_TYPE_JOB_OUTPUT_PATH = BASE_PATH + "/GET_TOTAL_WORD_COUNT_FROM_DOC_TYPE_JOB_OUTPUT";

    // 测试集的贝叶斯分类结果目录
    public static final String GET_NAIVE_BAYES_RESULT_JOB_OUTPUT_PATH = BASE_PATH + "/GET_NAIVE_BAYES_RESULT_JOB_OUTPUT";

    // Hadoop生成的文件名 因为本实验处理的文件大小都小于block_size所以只有一个part文件
    public static final String HADOOP_DEFAULT_OUTPUT_FILE_NAME = "/part-r-00000";

    public static final String DOC_TYPE_LIST = "CHINA@JAP";

    public static final List<String> STOP_WORDS_LIST = Arrays.asList(stopWordsArray);
}
