package MapReduce.EmpDept.DeptSalary;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

public class Map extends Mapper<LongWritable, Text, Text, Bean> {
    private HashMap<String, String> deptMap = new HashMap<>();
    Bean bean = new Bean();
    Text text = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //获取缓存文件，并且封装到集合,dept
        URI[] cacheFile = context.getCacheFiles();

        FileSystem fileSystem = FileSystem.get(context.getConfiguration());
        FSDataInputStream fsDataInputStream = fileSystem.open(new Path(cacheFile[0]));

        //读取数据
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fsDataInputStream, "UTF-8"));

        String line;
        while(StringUtils.isNotEmpty(line = bufferedReader.readLine())){
            String[] split = line.split(",");

            //复制
            deptMap.put(split[0],split[1]);
        }

        //关闭流
        IOUtils.closeStream(bufferedReader);

    }

    public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException{

        String[] split = value.toString().split(",");

        if (split[6].length() == 0 || split[6].length() == 1 ){
            split[6] = "0";
        }

        String DeptNum =deptMap.get(split[0]);

        String EmpName = split[1];
        String EmpData = split[4];
        int EmpSalary_one = Integer.parseInt(split[5]);
        int EmpSalary_two = Integer.parseInt(split[6]);


        bean.setDeptNum(DeptNum);
        bean.setEmpName(EmpName);
        bean.setEmpData(EmpData);
        bean.setEmpSalay_one(EmpSalary_one);
        bean.setEmpSalay_two(EmpSalary_two);
        bean.setEmpSalay(EmpSalary_one + EmpSalary_two);
        bean.setDeptName(deptMap.get(split[1]));

        text.set(deptMap.get(split[1]));

        context.write(text,bean);

    }

}
