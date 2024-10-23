package wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException,
            IllegalArgumentException, ClassCastException, ClassNotFoundException, InterruptedException {
        // 加载配置类
        Configuration conf = new Configuration();

        // 检查参数数量
        if (args.length < 2) {
            System.err.println("Usage: wordcount.Main <input path> <word output path>");
            System.exit(-1);
        }

        // 任务2：统计热点新闻标题中的高频单词
        Job wordJob = Job.getInstance(conf, "Word Count");
        wordJob.setJarByClass(Main.class);
        wordJob.setMapperClass(WordCountMapper.class);
        wordJob.setReducerClass(WordCountReducer.class);
        wordJob.setMapOutputKeyClass(Text.class);
        wordJob.setMapOutputValueClass(IntWritable.class);
        wordJob.setOutputKeyClass(Text.class);
        wordJob.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(wordJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(wordJob, new Path(args[1]));

        // 删除现有的输出目录
        FileSystem fs = FileSystem.get(conf);
        Path wordOutputPath = new Path(args[1]);
        if (fs.exists(wordOutputPath)) {
            fs.delete(wordOutputPath, true);
        }

        System.exit(wordJob.waitForCompletion(true) ? 0 : 1);
    }
}