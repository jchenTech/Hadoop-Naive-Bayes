package job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import utils.Const;

import java.io.IOException;

/**
 * 根据GetSingleWordCountFromDocTypeJob输出的sequence_file统计每个文档类别的总单词数
 */
public class GetTotalWordCountFromDocTypeJob extends Configured implements Tool {

    public static class GetTotalWordCountFromDocTypeMapper extends Mapper<Text, IntWritable, Text, IntWritable> {
        private Text docTypeName = new Text();
        // 该文档中每个单词出现的总次数
        private IntWritable wordCount = new IntWritable(0);

        /*
         * 重写map函数，输入为GetSingleWordCountFromDocTypeJob输出的SequenceFile，输出为<文档类型，单词数量>
         */
        @Override
        protected void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
            // key: CANA@hello
            // value: 13 表示hello在CANA文档类别中出现了13次
            String docTypeName = key.toString().split("@")[0];
            this.docTypeName.set(docTypeName);
            this.wordCount.set(value.get());
            context.write(this.docTypeName, this.wordCount);
        }
    }

    public static class GetTotalWordCountFromDocTypeReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        // 每个文档类别的单词总词数
        private IntWritable totalWordCount = new IntWritable(0);

        /*
         * 重写reduce函数，输入为<文档类型，[count1,count2,...]>，输出为<文档类型，该类型中的单词总数>
         */
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            // key: CANA
            // values: [13,1,1,24,3,7....12,3,6]
            // values是中该类别中每个单词出现的次数组成的数组，数组求和即是每个文档类别的单词总词数
            int totalWordCount = 0;
            for (IntWritable wordCount : values) {
                totalWordCount += wordCount.get();
            }
            this.totalWordCount.set(totalWordCount);
            System.out.println(key.toString() + this.totalWordCount);
            context.write(key, this.totalWordCount);
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        System.out.println("开始对 GetTotalWordCountFromDocTypeJob 进行配置");

        Configuration conf = new Configuration();

        // 如果输出目录存在，则先删除输出目录
        Path outputPath = new Path(Const.GET_TOTAL_WORD_COUNT_FROM_DOC_TYPE_JOB_OUTPUT_PATH);
        FileSystem fs = outputPath.getFileSystem(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        Job job = Job.getInstance(conf, "GetTotalWordCountFromDocTypeJob");

        job.setJarByClass(GetTotalWordCountFromDocTypeJob.class);
        job.setMapperClass(GetTotalWordCountFromDocTypeJob.GetTotalWordCountFromDocTypeMapper.class);
        job.setCombinerClass(GetTotalWordCountFromDocTypeJob.GetTotalWordCountFromDocTypeReducer.class);
        job.setReducerClass(GetTotalWordCountFromDocTypeJob.GetTotalWordCountFromDocTypeReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(Const.GET_SINGLE_WORD_COUNT_FROM_DOC_TYPE_JOB_OUTPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(Const.GET_TOTAL_WORD_COUNT_FROM_DOC_TYPE_JOB_OUTPUT_PATH));

        System.out.println("完成配置，开始执行 GetTotalWordCountFromDocTypeJob");
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new GetTotalWordCountFromDocTypeJob(), args);
        System.out.println("GetTotalWordCountFromDocTypeJob 运行结束, 已计算所有文档类型中所有单词出现的次数");
        System.exit(res);
    }
}
