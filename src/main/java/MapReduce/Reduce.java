package MapReduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    IntWritable intWritable = new IntWritable();

    public void reduce(Text	key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
        int sum = 0;
        for (IntWritable intWritable : values ){
             sum += intWritable.get();
        }
        intWritable.set(sum);
        context.write(key, intWritable);
    }
}
