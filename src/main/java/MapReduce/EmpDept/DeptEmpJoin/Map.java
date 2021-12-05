package MapReduce.EmpDept.DeptEmpJoin;

import MapReduce.EmpDept.DeptEmpJoin.Bean;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Properties;

public class Map extends Mapper<LongWritable, Text, Text, Bean> {
//    MapWritable deptMap = new MapWritable();
    Bean bean = new Bean();
    Text text = new Text();

    @Override
//    protected void setup(Context context) throws IOException, InterruptedException {
//        //获取缓存文件，并且封装到集合,dept
//        URI[] cacheFile = context.getCacheFiles();
//
//        FileSystem fs = FileSystem.get(context.getConfiguration());
//        FSDataInputStream fsDataInputStream = fs.open(new Path(cacheFile[0]));
//
//
//        //读取数据
//        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fsDataInputStream, "UTF-8"));
//
//        String line;
//        while ((line = bufferedReader.readLine())  != null) {
//            String[] split = line.split(",");
//
//            //复制
//            deptMap.put(split[0], split[1]);
//            System.out.println(deptMap);
//        }
//        //关闭流
//        bufferedReader.close();
//    }

//
//    @Override
//    protected void setup(Context context) throws IOException, InterruptedException {
//
//        FileSplit fileSplit = (FileSplit) context.getInputSplit();
//        Path path = fileSplit.getPath();
//
//        fileName = path.getName();
//    }

    public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException{

        //7369,SMITH,CLERK,7902,1980/12/17,800,,
        String[] split = value.toString().split(",");

        if(split.length == 8) {
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
            bean.setDeptNum(split[7]);
            bean.setFlage("emp");

            text.set(split[7]);
        }else {
            bean.setDeptName(split[1]);
            bean.setDeptNum(split[0]);
            bean.setFlage("dept");
            text.set(split[0]);
        }
            context.write(text,bean);
    }
}
