package job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.Const;

import java.util.HashMap;
import java.util.Map;

/**
 * 对各文档的贝叶斯分类结果进行评估，计算各文档FP、TP、FN、TN、Precision、Recall、F1以及整体的宏平均、微平均。
 */
public class Evaluation extends Configured implements Tool {

    // 每个文档的预测结果
    private static Map<String, String> docPredictResultMap = new HashMap<>();

    // 文档种类列表
    private static String[] docTypeList;

    public static final Logger log = LoggerFactory.getLogger(Evaluation.class);

    public static void doEvaluation() {
        docTypeList = Const.DOC_TYPE_LIST.split("@");
        Path bayesResult = new Path(Const.GET_NAIVE_BAYES_RESULT_JOB_OUTPUT_PATH + Const.HADOOP_DEFAULT_OUTPUT_FILE_NAME);
        Configuration conf = new Configuration();
        SequenceFile.Reader reader = null;
        try {
            SequenceFile.Reader.Option option = SequenceFile.Reader.file(bayesResult);
            reader = new SequenceFile.Reader(conf, option);
            Text sequenceKey = new Text();
            // key: CHINA@481518newsML.txt
            Text sequenceValue = new Text();
            // value: CANA@-1953.9381329830253
            while (reader.next(sequenceKey, sequenceValue)) {
                docPredictResultMap.put(sequenceKey.toString(), sequenceValue.toString());
            }
            double totalPrecision = 0, totalRecall = 0, totalF1 = 0;
            double totalTP = 0, totalTN = 0, totalFP = 0, totalFN = 0;
            for (String c : docTypeList) {
                double TP = 0, TN = 0, FP = 0, FN = 0;
                for (String key : docPredictResultMap.keySet()) {
                    String value = docPredictResultMap.get(key);
                    String realDocType = key.split("@")[0];
                    String predictDocType = value.split("@")[0];
                    if (realDocType.equals(c) && predictDocType.equals(c)) {
                        TP++;
                    } else if (realDocType.equals(c)) {
                        FN++;
                    } else if (predictDocType.equals(c)) {
                        FP++;
                    } else {
                        TN++;
                    }
                }
                double precision = TP / (TP + FP);
                totalPrecision += precision;
                double recall = TP / (TP + FN);
                totalRecall += recall;
                double f1 = 2 * precision * recall / (precision + recall);
                totalF1 += f1;
                totalTP += TP;
                totalFN += FN;
                totalTN += TN;
                totalFP += FP;
                System.out.print(c + " TP= " + TP);
                System.out.print(" FN= " + FN);
                System.out.print(" FP= " + FP);
                System.out.println(" TN= " + TN);
                System.out.println(c + " precision: " + precision);
                System.out.println(c + " recall: " + recall);
                System.out.println(c + " f1: " + f1);
                System.out.println();
            }
            double precision = totalTP / (totalTP + totalFP);
            double recall = totalTP / (totalTP + totalFN);
            double f1 = 2 * precision * recall / (precision + recall);
            System.out.print("Total TP= " + totalTP);
            System.out.print("Total FN= " + totalFN);
            System.out.print("Total FP= " + totalFP);
            System.out.println("Total TN= " + totalTN);
            System.out.println();

            System.out.println("微平均");
            System.out.println("Precision: " + precision);
            System.out.println("Recall: " + recall);
            System.out.println("F1: " + f1);
            System.out.println();
            System.out.println("宏平均");
            System.out.println("Precision: " + totalPrecision / docTypeList.length);
            System.out.println("Recall: " + totalRecall / docTypeList.length);
            System.out.println("F1: " + totalF1 / docTypeList.length);

        } catch (Exception ex) {
            log.error(ex.getMessage());
        } finally { // 确保发生异常时关闭reader
            IOUtils.closeStream(reader);
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        doEvaluation();
        System.out.println("已计算测试集中各文档的贝叶斯分类结果");
        return 0;
    }
}
