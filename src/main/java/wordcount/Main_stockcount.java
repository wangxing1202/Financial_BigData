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
        // 检查参数数量
        if (args.length < 2) {
            System.err.println("Usage: wordcount.Main <input path> <stock output path>");
            System.exit(-1);
        }

        // 加载配置类
        Configuration conf = new Configuration();

        // 任务1：统计股票代码出现次数
        Job stockJob = Job.getInstance(conf, "Stock Count");
        stockJob.setJarByClass(Main.class);
        stockJob.setMapperClass(StockCountMapper.class);
        stockJob.setReducerClass(StockCountReducer.class);
        stockJob.setMapOutputKeyClass(Text.class);
        stockJob.setMapOutputValueClass(IntWritable.class);
        stockJob.setOutputKeyClass(Text.class);
        stockJob.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(stockJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(stockJob, new Path(args[1]));

        // 删除现有的输出目录
        FileSystem fs = FileSystem.get(conf);
        Path outputPath = new Path(args[1]);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        System.exit(stockJob.waitForCompletion(true) ? 0 : 1);
    }
}