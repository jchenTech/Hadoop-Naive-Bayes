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
 * 根据InitSequenceFileJob输出的sequence_file统计每个DocType有多少个文档
 */
public class GetDocCountFromDocTypeJob extends Configured implements Tool {

    public static class GetDocCountMapper extends Mapper<Text, BytesWritable, Text, IntWritable> {
        private Text docTypeName = new Text();
        private IntWritable docCount = new IntWritable(1);

        /*
         * 重写map函数，输入为<文档类型@文件名，文件内容>，输出为<文档类型，1>
         */
        @Override
        protected void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
            // key: CANA@487557newsML.txt
            // value: 487557newsML.txt的文件内容
            // 这里只取key的信息用来计算每个文档种类有多少个文档，value不用管
            String[] keyName = key.toString().split("@");
            this.docTypeName.set(keyName[0]);
            this.docCount.set(1);
            context.write(this.docTypeName, docCount);
        }
    }

    public static class GetDocCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private Text docTypeName = new Text();
        private IntWritable totalDocCount = new IntWritable(1);

        /*
         * 重写reduce方法，输入为<文档类型，[1,1,...]>，输出为<文档类型，该类型的文件总数>
         */
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            // key: CANA
            // values: [1,1,1,1,1.....,1,1,1]
            // 因为设置了job.setCombinerClass(GetDocCountReducer.class);
            // 相同的key的value会合并成一个数组，数组的和就是改文档种类对应的文档总数
            int totalDocCount = 0;
            for (IntWritable docCount : values) {
                totalDocCount += docCount.get();
            }
            this.docTypeName.set(key);
            this.totalDocCount.set(totalDocCount);
            context.write(this.docTypeName, this.totalDocCount);
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        System.out.println("开始对 GetDocCountFromDocTypeJob 进行配置");

        Configuration conf = new Configuration();

        // 如果输出目录存在，则先删除输出目录
        Path outputPath = new Path(Const.GET_DOC_COUNT_FROM_DOC_TYPE_JOB_OUTPUT_PATH);
        FileSystem fs = outputPath.getFileSystem(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        Job job = Job.getInstance(conf, "GetDocCountFromDocTypeJob");

        job.setJarByClass(GetDocCountFromDocTypeJob.class);
        job.setMapperClass(GetDocCountMapper.class);
        job.setCombinerClass(GetDocCountReducer.class);
        job.setReducerClass(GetDocCountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(Const.TRAIN_DATA_SEQUENCE_FILE_PATH));
        FileOutputFormat.setOutputPath(job, new Path(Const.GET_DOC_COUNT_FROM_DOC_TYPE_JOB_OUTPUT_PATH));

        System.out.println("完成配置，开始执行 GetDocCountFromDocTypeJob");
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new GetDocCountFromDocTypeJob(), args);
        System.out.println("GetDocCountFromDocTypeJob 运行结束");
        System.exit(res);
    }
}
