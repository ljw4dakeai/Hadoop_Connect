package MapReduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class Map extends Mapper<Object, Text, Text, LongWritable>{
    LongWritable longWritable = new LongWritable();
    Text text = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
        StringTokenizer stringTokenizer = new StringTokenizer(value.toString());
        while (stringTokenizer.hasMoreTokens()){
            text.set(stringTokenizer.nextToken());
            context.write(text,longWritable);
        }
    }

}
