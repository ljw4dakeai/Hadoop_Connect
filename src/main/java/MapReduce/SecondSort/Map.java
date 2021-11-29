package MapReduce.SecondSort;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


import java.io.IOException;

public class Map extends Mapper<LongWritable, Text, Bean, IntWritable> {
    Bean bean = new Bean();
    IntWritable intWritable = new IntWritable();
    public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException {

        String[] split = value.toString().split(",");
        int first = Integer.parseInt(split[0]);
        int second = Integer.parseInt(split[1]);

        bean.setFirst(second); //写反
        bean.setSecond(first);
        intWritable.set(first);
        //重新组合（1000037 100 1000037）
        context.write(bean, intWritable);

    }
}
