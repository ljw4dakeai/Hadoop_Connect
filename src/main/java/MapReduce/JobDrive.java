package MapReduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Options;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;


import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;



public class JobDrive {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();

        //fs配置
        Properties properties = new Properties();
        properties.load(new FileInputStream("src/main/java/Hadoop/Hadoop.properties"));
        String URI =  properties.getProperty("URI");
        String user = properties.getProperty("user");
        FileSystem fs = FileSystem.get(new URI(URI), configuration,user);

        //dir配置
        String input = "hdfs://192.168.95.136:9000/user/zoujiahao1905140016/input.txt" ;
        String output = "hdfs://192.168.95.136:9000/user/zoujiahao1905140016/output.txt";
        Path inputpath = new Path(input);
        Path outputpath = new Path(output);

        //上传input，需要可以重新上传
//        Path localinput = new Path("/Users/zoujiahao/IDEA/Hadoop_Connect/src/main/java/MapReduce/localinput.txt");
//        fs.copyFromLocalFile(localinput, inputpath);


        //job配置
        Job job = Job.getInstance(configuration);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        //输入输出配置配置
        FileInputFormat.setInputPaths(job,inputpath);
        FileOutputFormat.setOutputPath(job,outputpath);

        boolean result = job.waitForCompletion(true);

        //输入输出文件内容
        FileInputStream fileInputStream = new FileInputStream("/Users/zoujiahao/IDEA/Hadoop_Connect/src/main/java/MapReduce/localinput.txt");
        System.out.println("input文件内容");
        String content = null;
        int size = 0;
        byte[] bytes = new byte[1024];
        while ((size = fileInputStream.read(bytes)) != -1 ){
            content = new String(bytes, 0, size);
            System.out.println(content);

        }

        System.exit(result ? 0 : 1);

    }
}
