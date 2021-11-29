package MapReduce.SecondSort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

// 分区函数类代码
public class Part extends Partitioner<Bean, IntWritable> {

    @Override
    public int getPartition(Bean bean, IntWritable intWritable, int i) {
        return Math.abs(bean.getFirst() * 127) % i;
    }
}
