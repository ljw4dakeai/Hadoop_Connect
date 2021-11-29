package MapReduce.LogURL_Number;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


import java.io.IOException;

public class Map extends Mapper<LongWritable, Text, Text, Bean> {

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    Text text = new Text();
    Bean bean = new Bean();

    public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException {
        String[] split = value.toString().split("\\s"); //"\\s+表示，匹配空格或者制表符或者换页符"
        String subject = split[1];

        //数据清洗

        if (split.length != 5){
            return;
        }
        if (split[2].length() == 0){
            split[2] = "0";
            return;
        }
        if (split[3].length() == 0){
            split[3] = "0";
            return;
        }
        if ( !split[2].equalsIgnoreCase("2")  || !split[3].equalsIgnoreCase("1")){
            return;
        }

        String id = split[0];
        int num = Integer.parseInt(split[2]);
        int list = Integer.parseInt(split[3]);
        text.set(subject);

        bean.setID(id);
        bean.setNum(num);
        bean.setList(list);
        bean.setUrl(split[4]);

        context.write(text,bean);

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
