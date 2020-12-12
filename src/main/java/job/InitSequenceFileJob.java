package job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Arrays;

/**
 * 将.txt格式的输入文件转换成SequenceFile
 * 将"文档类型@文件名"作为SequenceFile的Key进行保存
 */
public class InitSequenceFileJob extends Configured implements Tool {

    /**
     * 重写Mapper函数
     */
    public static class InitSequenceFileMapper extends Mapper<NullWritable, BytesWritable, Text, BytesWritable> {
        private Text docKey = new Text();

        /**
         * 在map之前运行,将map的key映射成：文档类型@文件名
         * 例如 CANA@477888newsML.txt
         */
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
//            System.out.println("start InitSequenceFileMapper's setup()");
            InputSplit split = context.getInputSplit();
            String docName = ((FileSplit) split).getPath().getName();
            String docType = ((FileSplit) split).getPath().getParent().getName();
            docKey.set(docType + "@" + docName);
//            System.out.println("将map的key映射成：" + docType + "@" + docName);
        }

        /**
         * 重写map函数，返回key-value对为 <文档类型@文件名，文件内容>
         */
        @Override
        protected void map(NullWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
            // key: CANA@487557newsML.txt
            // value: 487557newsML.txt的文件内容
//            System.out.println("start InitSequenceFileMapper's map()");
            context.write(this.docKey, value);
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        System.out.println("开始对 InitSequenceFileJob 进行配置");
        Configuration conf = getConf();
        Path inputPath = new Path(conf.get("INPUT_PATH"));
        Path outputPath = new Path(conf.get("OUTPUT_PATH"));

        FileSystem fs = outputPath.getFileSystem(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        fs = inputPath.getFileSystem(conf);
        //获取到的是hdfs://master:8020/目录下的文件信息，即每个数据集文件夹的信息
        FileStatus[] inputFileStatusList = fs.listStatus(inputPath);
        String[] inputFilePathList = new String[inputFileStatusList.length];
        for (int i = 0; i < inputFilePathList.length; i++) {
            //获取每个数据集文件目录的路径，例如：hdfs://master:8020/CANA
            inputFilePathList[i] = inputFileStatusList[i].getPath().toString();
        }

        Job job = Job.getInstance(conf, "InitSequenceFileJob");

        job.setJarByClass(InitSequenceFileJob.class);
        job.setMapperClass(InitSequenceFileMapper.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);
        job.setInputFormatClass(InitSequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        // 将每个数据集文件夹导入到InitSequenceFileInputFormat中
        for (String path : inputFilePathList) {
            InitSequenceFileInputFormat.addInputPath(job, new Path(path));
        }
        SequenceFileOutputFormat.setOutputPath(job, outputPath);

        System.out.println("完成配置，开始执行 InitSequenceFileJob");
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new InitSequenceFileJob(), args);
        System.out.println("InitSequenceFileJob 运行结束");
        System.exit(exitCode);
    }
}
