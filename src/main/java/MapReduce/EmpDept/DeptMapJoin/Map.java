package MapReduce.EmpDept.DeptMapJoin;



import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;

public class Map extends Mapper<LongWritable, Text, Bean, NullWritable> {
    HashMap<String, String> deptMap = new HashMap<>();
    Bean bean = new Bean();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        URI[] cacheFiles = context.getCacheFiles();

        FileSystem fileSystem = FileSystem.get(context.getConfiguration());

        FSDataInputStream fsDataInputStream = fileSystem.open(new Path(cacheFiles[0]));

        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fsDataInputStream,"UTF-8"));

        String string ;
        while ((string = bufferedReader.readLine()) != null){
            String[] split = string.split(",");

            deptMap.put(split[0], split[1] );
        }
        bufferedReader.close();
    }

    public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException{
        //7369,SMITH,CLERK,7902,1980/12/17,800,,10
        String[] split = value.toString().split(",");
        if (split[6].length() == 0 || split[6].length() == 1) {
            split[6] = "0";
        }
        String EmpName = split[1];
        String EmpData = split[4];
        int EmpSalary_one = Integer.parseInt(split[5]);
        int EmpSalary_two = Integer.parseInt(split[6]);

        bean.setEmpName(EmpName);
        bean.setEmpData(EmpData);
        bean.setEmpSalay_one(EmpSalary_one);
        bean.setEmpSalay_two(EmpSalary_two);
        bean.setEmpSalay(EmpSalary_one + EmpSalary_two);
        bean.setDeptName(deptMap.get(split[7]));
        bean.setDeptNum(split[7]);
        bean.setEmpName(split[1]);

        context.write(bean, NullWritable.get());


    }

}
