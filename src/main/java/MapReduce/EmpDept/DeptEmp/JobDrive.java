package MapReduce.EmpDept.DeptEmp;

import MapReduce.EmpDept.DeptEmpJoin.Bean;
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

public  class JobDrive {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();

        //fs配置
        Properties properties = new Properties();
        properties.load(new FileInputStream("src/main/java/Hadoop/Hadoop.properties"));
        String URI = properties.getProperty("URI");
        String user = properties.getProperty("user");
        FileSystem fs = FileSystem.get(new URI(URI), configuration, user);
//        DistributedCache.addCacheFile(new Path("hdfs://192.168.95.136:9000/user/zoujiahao1905140016/MapReduce/EmpDept/dept.csv").toUri(),configuration);

        //configuration配置，不会影响上面的fs配置
        configuration.set("fs.defaultFS", "hdfs://192.168.95.136:9000");
        configuration.set("mapred.job.tracker", "192.168.95.136:9001");
        configuration.set("hadoop.job.user", "root");
        configuration.set("mapreduce.framework.name", "yarn");
        configuration.set("yarn.resourcemanager.hostname", "node0");
        configuration.set("yarn.nodemanager.aux-services", "mapreduce_shuffle");
        configuration.set("hadoop.tmp.dir", "/data/hadoop/tmp");
        configuration.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());


        String input = "hdfs://192.168.95.136:9000/user/zoujiahao1905140016/MapReduce/EmpDept/output/part-r-00000";
        String output = "hdfs://192.168.95.136:9000/user/zoujiahao1905140016/MapReduce/EmpDept/Dept/output";

        Path inputpath = new Path(input);
        Path outputpath = new Path(output);

        //运行前删除outpath
        fs.delete(new Path(output));

        String jar = "/Users/zoujiahao/IDEA/Hadoop_Connect/out/artifacts/Hadoop_Connect_MapReduce_DeptEmp_jar/Hadoop_Connect_MapReduce_DeptEmp.jar";

        //job配置
        Job job = Job.getInstance(configuration);

        job.setJarByClass(JobDrive.class);
        job.setJar(jar);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Bean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        FileInputFormat.setInputPaths(job,inputpath);
        FileOutputFormat.setOutputPath(job,outputpath);
        boolean result = job.waitForCompletion(true);


        System.out.println("input内容");
        FSDataInputStream fsDataInputStream = fs.open(new Path(input));
        InputStreamReader inputStreamReader = new InputStreamReader(fsDataInputStream);
        BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

        String string = bufferedReader.readLine();
        while (string != null){
            System.out.println(string);
            string = bufferedReader.readLine();
        }
        System.out.println("--------------------------------------------------------------------------------------------");
        System.out.println("output内容");
        FSDataInputStream fsDataInputStream1 = fs.open(new Path(output + "/part-r-00000"));
        InputStreamReader inputStreamReader1 = new InputStreamReader(fsDataInputStream1);
        BufferedReader bufferedReader1 = new BufferedReader(inputStreamReader1);

        String string1 = bufferedReader1.readLine();
        while (string1 != null){
            System.out.println(string1);
            string1 = bufferedReader1.readLine();
        }

        bufferedReader.close();
        inputStreamReader.close();
        fsDataInputStream.close();
        bufferedReader1.close();
        inputStreamReader1.close();
        fsDataInputStream1.close();
        fs.close();

        System.exit(result ? 0 : 1);

    }
}
