package MapReduce.GrossProceeds;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Map extends Mapper<LongWritable, Text, Text, Bean> {
    Text text = new Text();
    Bean bean = new Bean();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(",");
        String Data = split[2];
        String[] strings = Data.split("-");

        String ID = split[0];
        String year= strings[0];
        String Num = split[5];
        String Total = split[6];

        int id = Integer.parseInt(ID);
        int num = Integer.parseInt(Num);
        float total = Float.parseFloat(Total);

        text.set(year + "年");

        bean.setTotal(total);
        bean.setNum(num);
        bean.setID(id);

        context.write(text, bean);

    }
}
