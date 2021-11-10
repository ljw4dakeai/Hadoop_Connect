package MapReduce.TopScore;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Reduce extends Reducer<Text, IntWritable,Text, Text> {
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //初始化最高分，最低分，有多少个，求和值平均值
        int Max = 0; //遇到比0大的就替换
        int Min = 1000;//遇到比100小的就替换
        int count = 0;//++
        double sum = 0;// sum += sum;
        double avg = 0; //avg= sum/count

        for (IntWritable value : values){
            //最大值
            if (Max < value.get()){
                Max = value.get();
            }

            //最小值
            if (Min > value.get()){
                Min = value.get();
            }

            //求平均
            count ++ ;
            sum += value.get();
            avg = sum / count * 1f;
        }

        String out = "最高分 = " + Max + "\t" + "最低分 = " + Min + "\t" + "平均分 = " + avg;
        context.write(key, new Text(out));
    }
}
