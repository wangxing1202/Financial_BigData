package wordcount;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

public class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private Set<String> stopWords = new HashSet<>();

    protected void setup(Context context) throws IOException, InterruptedException {
        // 停词文件的固定路径
        Path stopWordsPath = new Path("D:/JetBrains/input/stop-word-list.txt");

        // 读取停词文件
        FileSystem fs = FileSystem.get(context.getConfiguration());
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(stopWordsPath)));
        String line;
        while ((line = br.readLine()) != null) {
            stopWords.add(line.trim().toLowerCase());
        }
        br.close();
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        if (fields.length > 1) {
            String headline = fields[1].toLowerCase().replaceAll("[^a-zA-Z\\s]", " ");
            StringTokenizer itr = new StringTokenizer(headline);
            while (itr.hasMoreTokens()) {
                String token = itr.nextToken();
                if (!stopWords.contains(token)) {
                    word.set(token);
                    context.write(word, one);
                }
            }
        }
    }
}