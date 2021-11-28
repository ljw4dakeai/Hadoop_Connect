package MapReduce.GrossProceeds;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


import java.io.IOException;

public class Reduce extends Reducer<Text, Bean,Text, Text> {

    public void reduce(Text key, Iterable<Bean> values, Context context) throws IOException, InterruptedException {
        int num = 0;
        float total = 0;

        for (Bean value:values) {
            num += value.getNum();
            total += value.getTotal();
        }

        String total_all = "销售数量：" + num + "个" + "\t" + "销售总额：" + total + "元";

        context.write(key, new Text(total_all));

    }
}
