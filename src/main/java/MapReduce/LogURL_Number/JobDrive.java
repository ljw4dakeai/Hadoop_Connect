package MapReduce.LogURL_Number;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Properties;

public class JobDrive {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();


        //fs配置
        Properties properties = new Properties();
        properties.load(new FileInputStream("src/main/java/Hadoop/Hadoop.properties"));
        String URI = properties.getProperty("URI");
        String user = properties.getProperty("user");
        FileSystem fs = FileSystem.get(new URI(URI), configuration, user);

        //configuration配置，不会影响上面的fs配置
        configuration.set("fs.defaultFS", "hdfs://192.168.95.136:9000");
        configuration.set("mapred.job.tracker", "192.168.95.136:9001");
        configuration.set("hadoop.job.user", "root");
        configuration.set("mapreduce.framework.name", "yarn");
        configuration.set("yarn.resourcemanager.hostname", "node0");
        configuration.set("yarn.nodemanager.aux-services", "mapreduce_shuffle");
        configuration.set("hadoop.tmp.dir", "/data/hadoop/tmp");
        configuration.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());


        //dir配置
        String input = "hdfs://192.168.95.136:9000/user/zoujiahao1905140016/MapReduce/LogURL_Number/input.filter" ;
        String output = "hdfs://192.168.95.136:9000/user/zoujiahao1905140016/MapReduce/LogURL_Number/output";
        Path inputpath = new Path(input);
        Path outputpath = new Path(output);


        //运行前删除outpath
        fs.delete(new Path(output));


        //上传input，需要可以重新上传
        Path localinputpath = new Path("/Users/zoujiahao/IDEA/Hadoop_Connect/src/main/java/MapReduce/LogURL_Number/localinput.filter");
        Path localoutputpath = new Path("/Users/zoujiahao/IDEA/Hadoop_Connect/src/main/java/MapReduce/LogURL_Number/localoutput");


        String jar = "/Users/zoujiahao/IDEA/Hadoop_Connect/out/artifacts/Hadoop_Connect_MapReduce_LogURL_Number_jar/Hadoop_Connect_MapReduce_LogURL_Number.jar";


        //job配置
        Job job = Job.getInstance(configuration);

        //设置jar包
        job.setJarByClass(MapReduce.LogURL_Number.JobDrive.class);
        job.setJar(jar);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapReduce.LogURL_Number.Bean.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MapReduce.LogURL_Number.Bean.class);
        job.setMapperClass(MapReduce.LogURL_Number.Map.class);

        FileInputFormat.setInputPaths(job,inputpath);
        FileOutputFormat.setOutputPath(job,outputpath);
        boolean result = job.waitForCompletion(true);

        fs.copyToLocalFile(outputpath,localoutputpath);

        fs.close();


        System.exit(result ? 0 : 1);


    }
}
