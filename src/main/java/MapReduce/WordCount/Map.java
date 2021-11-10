package MapReduce.WordCount;

//Java类型	Hadoop Writable类型
//boolean	BooleanWritable
//byte	    ByteWritable
//int	    IntWritable
//float	    FloatWritable
//long	    LongWritable
//double	DoubleWritable
//String	Text
//map	    MapWritable
//array	    ArrayWritable

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;



public class Map extends Mapper<Object, Text, Text, IntWritable>{
    IntWritable IntWritable = new IntWritable();
    Text text = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
        StringTokenizer stringTokenizer = new StringTokenizer(value.toString());
        while (stringTokenizer.hasMoreTokens()){
            this.text.set(stringTokenizer.nextToken());
            context.write(this.text,IntWritable);
        }
    }

}
