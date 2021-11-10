package MapReduce.TopScore;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    Text text = new Text();
    IntWritable intWritable = new IntWritable();

    public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException {
        String[] split = value.toString().split(" ");
        String subject = split[0];
        int score = Integer.parseInt(split[1]);
        text.set(subject);
        intWritable.set(score);
        context.write(text, intWritable);

    }

}
