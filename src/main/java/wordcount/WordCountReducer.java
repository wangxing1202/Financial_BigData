package wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private Map<Text, IntWritable> countMap = new HashMap<>();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        countMap.put(new Text(key), new IntWritable(sum));
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
        List<Map.Entry<Text, IntWritable>> sortedEntries = new ArrayList<>(countMap.entrySet());
        sortedEntries.sort((e1, e2) -> e2.getValue().compareTo(e1.getValue()));

        int rank = 1;
        for (Map.Entry<Text, IntWritable> entry : sortedEntries.subList(0, Math.min(100, sortedEntries.size()))) {
            context.write(new Text(rank + ": " + entry.getKey()), entry.getValue());
            rank++;
        }
    }
}