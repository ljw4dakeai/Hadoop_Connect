  <h2 data-tool="mdnice编辑器" style="margin-top: 30px; margin-bottom: 15px; padding: 0px; font-weight: bold; color: black; border-bottom: 2px solid rgb(239, 112, 96); font-size: 1.3em;">
    <span class="prefix" style="display: none;"></span
    ><span
      class="content"
      style="display: inline-block; font-weight: bold; background: rgb(239, 112, 96); color: #ffffff; padding: 3px 10px 1px; border-top-right-radius: 3px; border-top-left-radius: 3px; margin-right: 3px;"
    >
      MapReduce
    </span> MAC端IDEA发送JAR给集群运行
    <span class="suffix"></span>
    <span style="display: inline-block; vertical-align: bottom; border-bottom: 36px solid #efebe9; border-right: 20px solid transparent;"></span>
  </h2>
  
  
##   引言
>当时想不通过自己手动传送JAR包给集群，我就爬了爬帖子，看看真么样可以远端执行，但是大部分都是windos系统下远端执行，很少有MacOS系统下的操作，但是道理都是一样的！但是还是记下来方便以后学习！


[「远端提交，JobDrive」](#JUMP)
 
## 一、本机环境变量配置
<mark>需要一个和虚拟机一样版本的Hadoop，解压到自己想要的位置！但是这个不需要配置虚拟机上etc目录下的那么多文件！只需要修改Hadoop-env.sh

1. 设置zsh配置

	>MacOS中的Java安装在/Library/Java/中，如果安装了多个版本的Java，只需要zsh中修改一下版本号就行了

		vi ~/.zshrc		
		
		JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.0_202.jdk/Contents/Home/

		CLASSPAHT=.:$JAVA_HOME/lib/dt.jar:$JAVA_HOME/lib/tools.jar
		
		PATH=$JAVA_HOME/bin:$PATH:
		
		export JAVA_HOME
		
		export CLASSPATH
		
		export PATH

2. 修改Hadoop.env.sh

	>配置本机的JAVA_HOME和HADOOP_HOME,本机的他们的位置

		export JAVA_HOME=/Library/java/JavaVirtualMachines/jdk1.8.0_202.jdk/Contents/Home
		export HADOOP_HOME=/Users/zoujiahao/hadoop/hadoop-3.2.2
		export HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop
		
		export HDFS_NAMENODE_USER=root
		export HDFS_DATANODE_USER=root
		export HDFS_SECONDARYNAMENODE_USER=root
		export YARN_RESOURCEMANAGER_USER=root
		export YARN_NODEMANAGER_USER=root
		
		
## 程序环境变量配置

>程序写好了之后，配置main环境变量


1. JobDrive环境变量配置

	>HADOOP_USER_NAME=root;
	>HADOOP_BIN_PATH=/Users/zoujiahao/hadoop/hadoop-3.2.2/bin;
	>HADOOP_HOME=/Users/zoujiahao/hadoop/hadoop-3.2.2

	![](https://s4.ax1x.com/2021/12/06/osdJKI.png)
	![](https://s4.ax1x.com/2021/12/06/osdYrt.png)
	
2. JAR配置

	><mark>需要在程序里面新建一个recourse文件夹用来存放
	
	![](https://s4.ax1x.com/2021/12/06/osdjiD.png)
	
3. 根目录下我放入了虚拟机中的etc中的五个配置文件

	>我觉得不需要配置....
	>
	>主要配置文件:
	>
	>详情请看：
	><https://www.cnblogs.com/ljw4dakeai/articles/15642346.html>
	(特别是最后的新加的环境变量)
	
	<mark>需要注意的地方！！！！</mark>
	
	<mark>进阶配置需要加上</mark>
	
3. 这里直接贴上WordCount代码

	<mark>主要是main函数去管理发送，其他的类似</mark>
	
	>Map
	
		package MapReduce.WordCount;
		
		//Java类型	Hadoop Writable类型
		//boolean	BooleanWritable
		//byte	    ByteWritable
		//int	    IntWritable
		//float	    FloatWritable
		//long	    LongWritable
		//double	DoubleWritable
		//String	Text
		//map	    MapWritable
		//array	    ArrayWritable
		
		import org.apache.hadoop.io.IntWritable;
		import org.apache.hadoop.io.LongWritable;
		import org.apache.hadoop.io.Text;
		import org.apache.hadoop.mapreduce.Mapper;
		
		import java.io.IOException;
		import java.util.StringTokenizer;
		
		
		
		public class Map extends Mapper<Object, Text, Text, IntWritable>{
		    IntWritable IntWritable = new IntWritable();
		    Text text = new Text();
	
	    public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
	        StringTokenizer stringTokenizer = new StringTokenizer(value.toString());
	        while (stringTokenizer.hasMoreTokens()){
	            this.text.set(stringTokenizer.nextToken());
	            context.write(this.text,IntWritable);
	        }
	    }
	
		}
		
		
		
	>Reduce
	
		package MapReduce.WordCount;
		
		import org.apache.hadoop.io.IntWritable;
		import org.apache.hadoop.io.Text;
		import org.apache.hadoop.mapreduce.Reducer;
		
		import java.io.IOException;
		
		public class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		    IntWritable intWritable = new IntWritable();
		
		    public void reduce(Text	key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
		        int sum = 0;
		        for (IntWritable intWritable : values ){
		             sum += intWritable.get();
		        }
		        intWritable.set(sum);
		        context.write(key, intWritable);
		    }
		}
		
		
		
	> <span id="jump">JobDrive</span>
	
	
		package MapReduce.WordCount;
	
		import org.apache.hadoop.conf.Configuration;
		import org.apache.hadoop.fs.FSDataInputStream;
		import org.apache.hadoop.fs.FileSystem;
		import org.apache.hadoop.fs.Path;
		import org.apache.hadoop.io.IntWritable;
		import org.apache.hadoop.io.Text;
		import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
		import org.apache.hadoop.mapreduce.Job;
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
        String URI =  properties.getProperty("URI");
        String user = properties.getProperty("user");
        FileSystem fs = FileSystem.get(new URI(URI), configuration,user);

        //configuration配置，不会影响上面的fs配置
        configuration.set("fs.defaultFS", "hdfs://主机IP:9000");
        configuration.set("mapred.job.tracker","主机IP:9001");
        configuration.set("hadoop.job.user","root");
        configuration.set("mapreduce.framework.name", "yarn");
        configuration.set("yarn.resourcemanager.hostname", "主机名");
        configuration.set("yarn.nodemanager.aux-services", "mapreduce_shuffle");
        configuration.set("hadoop.tmp.dir", "/data/hadoop/tmp");
        configuration.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());

        //dir配置
        String input = "hdfs://主机IP:9000/MapReduce/WordCount/input.txt" ;
        //运行前请上传文件
        String output = "hdfs://主机IP:9000/MapReduce/WordCount/output";
        Path inputpath = new Path(input);
        Path outputpath = new Path(output);

        //运行前删除outpath
        fs.delete(new Path(output));


        String jar = "构建JAR包的绝对路径";
        //构建方法：顶部：构建 -> 构建工件 -> 自己选择


        //job配置
        Job job = Job.getInstance(configuration);

        //设置jar包
        job.setJarByClass(MapReduce.WordCount.JobDrive.class);
        job.setJar(jar);


        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapperClass(MapReduce.WordCount.Map.class);
        job.setReducerClass(MapReduce.WordCount.Reduce.class);

        //输入输出配置配置
        FileInputFormat.setInputPaths(job,inputpath);
        FileOutputFormat.setOutputPath(job,outputpath);
        boolean result = job.waitForCompletion(true);
        
        System.exit(result ? 0 : 1);
	



