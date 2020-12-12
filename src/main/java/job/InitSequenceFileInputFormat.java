package job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.Arrays;

/**
 * 重写FileInputFormat和RecordReader函数
 * 将input的数据处理成bytes数组，作为InitSequenceFileJob中map的value
 */
public class InitSequenceFileInputFormat extends FileInputFormat<NullWritable, BytesWritable> {
    static {
        System.out.println("开始处理 InitSequenceFileJob 的 InputFormat");
    }

    @Override
    public RecordReader<NullWritable, BytesWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        InitSequenceFileRecordReader reader = new InitSequenceFileRecordReader();
//        System.out.println("start InitSequenceFileInputFormat's initialize() ");
        reader.initialize(inputSplit, taskAttemptContext);
        return reader;
    }
}

class InitSequenceFileRecordReader extends RecordReader<NullWritable, BytesWritable> {

    private FileSplit fileSplit;
    private Configuration conf;
    private BytesWritable value = new BytesWritable();
    private boolean processed = false;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        this.fileSplit = (FileSplit) inputSplit;
        this.conf = taskAttemptContext.getConfiguration();
        // initialize fileSplit:hdfs://master:8020/input/CANA/478888newsML.txt
    }

    /**
     * 重写nextKeyValue()函数，该函数会在Mapper中的map函数中赋值value的时候被调用
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (!processed) {
            byte[] contents = new byte[(int) fileSplit.getLength()];
            Path filePath = fileSplit.getPath();
            FileSystem fs = filePath.getFileSystem(conf);
            FSDataInputStream stream = null;
            try {
                stream = fs.open(filePath);
                // 将file文件中的内容放入contents数组中。
                // 使用了IOUtils实用类的readFully方法，将in流中得内容放入contents字节数组中。
                IOUtils.readFully(stream, contents, 0, contents.length);
                value.set(contents, 0, contents.length);
//                System.out.println("next value 完成：" + Arrays.toString(contents));
            } finally {
                IOUtils.closeStream(stream);
            }
            this.processed = true;
            return true;
        }
        return false;
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return processed ? 1.0f : 0.0f;
    }

    @Override
    public void close() throws IOException {

    }
}
