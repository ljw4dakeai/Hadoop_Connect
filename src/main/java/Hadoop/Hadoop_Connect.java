package Hadoop;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class Hadoop_Connect {

    //建立文件夹

    public void Mkdirs() throws IOException, InterruptedException, URISyntaxException {

        // 1 获取文件系统
        Configuration configuration = new Configuration();

        // 2 添加配置文件
        Properties properties = new Properties();
        properties.load(new FileInputStream("src/main/java/Hadoop/Hadoop.properties"));
        String URI =  properties.getProperty("URI");
        String user = properties.getProperty("user");
        FileSystem fs = FileSystem.get(new URI(URI), configuration, user);

        // 3 创建
        String Filename ;
        Scanner scanner = new Scanner(System.in);
        System.out.println("请输入你要创建的文件夹名称：例如：/user/zoujiahao1905140016/demo-three");
        Filename = scanner.next();

        fs.mkdirs(new Path(Filename));

        // 4 关闭资源
        fs.close();
    }

    //删除文件夹
    public void Delect() throws IOException, InterruptedException, URISyntaxException {

        // 1 获取文件系统
        Configuration configuration = new Configuration();

        // 2 添加配置文件
        Properties properties = new Properties();
        properties.load(new FileInputStream("src/main/java/Hadoop/Hadoop.properties"));
        String URI =  properties.getProperty("URI");
        String user = properties.getProperty("user");
        FileSystem fs = FileSystem.get(new URI(URI), configuration, user);

        // 3 删除
        String Filename ;
        Scanner scanner = new Scanner(System.in);
        System.out.println("请输入你要删除的文件夹名称：例如：/user/zoujiahao1905140016/demo-three");
        Filename = scanner.next();
        fs.delete(new Path(Filename));

        // 4 关闭资源
        fs.close();
    }

    //上传文件

    public void CopyFromLocalFile() throws IOException, InterruptedException, URISyntaxException {

        // 1 获取文件系统
        Configuration configuration = new Configuration();
        configuration.set("dfs.replication", "2");

        // 2 添加配置文件

        Properties properties = new Properties();
        properties.load(new FileInputStream("src/main/java/Hadoop/Hadoop.properties"));
        String URI =  properties.getProperty("URI");
        String user = properties.getProperty("user");
        FileSystem fs = FileSystem.get(new URI(URI), configuration,user);

        // 3 上传文件

        String filename ;
        Scanner scanner = new Scanner(System.in);
        System.out.println("请输入你要上传的文件名称：例如：e:/banzhang.txt");
        filename = scanner.next();

        String Filename ;
        Scanner scanner2 = new Scanner(System.in);
        System.out.println("请输入你要上传到的文件夹名称：例如：/user/zoujiahao1905140016/demo-three");
        Filename = scanner2.next();

        fs.copyFromLocalFile(new Path(filename), new Path(Filename));

        // 4 关闭资源
        fs.close();
    }

    // 下载文件

    public void CopyToLocalFile() throws IOException, InterruptedException, URISyntaxException{

        // 1 获取文件系统
        Configuration configuration = new Configuration();

        // 2 添加配置文件
        Properties properties = new Properties();
        properties.load(new FileInputStream("src/main/java/Hadoop/Hadoop.properties"));
        String URI =  properties.getProperty("URI");
        String user = properties.getProperty("user");
        FileSystem fs = FileSystem.get(new URI(URI), configuration,user);

        // 3 执行下载操作
        String filename ;
        Scanner scanner = new Scanner(System.in);
        System.out.println("请输入你要下载的文件名称：例如：/user/zoujiahao1905140016/demo-three/banzhang.txt");
        filename = scanner.next();

        String Filename ;
        Scanner scanner2 = new Scanner(System.in);
        System.out.println("请输入你要下载到的文件夹名称：例如：e:/");
        Filename = scanner2.next();

        // boolean delSrc 指是否将原文件删除
        // Path src 指要下载的文件路径
        // Path dst 指将文件下载到的路径
        // boolean useRawLocalFileSystem 是否开启文件校验
        fs.copyToLocalFile(false, new Path(filename), new Path(Filename), true);

        // 4 关闭资源
        fs.close();
    }

    //文件名称更改
    public void Rename() throws IOException, InterruptedException, URISyntaxException{

        // 1 获取文件系统
        Configuration configuration = new Configuration();

        // 2 添加配置文件
        Properties properties = new Properties();
        properties.load(new FileInputStream("src/main/java/Hadoop/Hadoop.properties"));
        String URI =  properties.getProperty("URI");
        String user = properties.getProperty("user");
        FileSystem fs = FileSystem.get(new URI(URI), configuration,user);

        // 3 修改文件名称
        String filename ;
        Scanner scanner = new Scanner(System.in);
        System.out.println("请输入原文件名称：例如：/user/zoujiahao1905140016/demo-three/banzhang.txt");
        filename = scanner.next();
        String refilename ;
        Scanner scanner2 = new Scanner(System.in);
        System.out.println("请输入修改后文件名称：例如：/user/zoujiahao1905140016/demo-three/bangzhan.txt");
        refilename = scanner2.next();
        fs.rename(new Path(filename), new Path(refilename));

        // 4 关闭资源
        fs.close();
    }



}


