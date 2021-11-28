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
        String ID = split[1];
        String Year = split[4];
        String[] strings = Year.split("/");
        String Total = split[6];

        int id = Integer.parseInt(ID);
        int year = Integer.parseInt(strings[1]);
        float total = Float.parseFloat(Total);

        text.set(String.valueOf(year));
        bean.setID(id);
        bean.setTotal(total);
        bean.setYear(year);

        context.write(text, bean);


    }
}