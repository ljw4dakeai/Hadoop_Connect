package MapReduce.SecondSort;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce extends Reducer<Bean, IntWritable, Text, IntWritable> {
    Text text = new Text();
    public void reduce(Bean bean, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //100
        text.set(Integer.toString(bean.getFirst()));

        for (IntWritable value:values){
            context.write(text,value);
        }

    }
}

