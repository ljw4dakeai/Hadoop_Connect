package Hadoop;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
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
        System.out.println("请输入你要创建的文件名称：例如：/user/zoujiahao1905140016/demo-three");
        Filename = scanner.next();

        if (fs.exists(new Path(Filename))){
            System.out.println("目录已经存在");
        }else {
            fs.mkdirs(new Path(Filename));
        }

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
        System.out.println("请输入你要删除的文件名称：例如：/user/zoujiahao1905140016/demo-three");
        Filename = scanner.next();
        if (!fs.exists(new Path(Filename))){
            System.out.println("要删除的文件不存在");
        }else {
            fs.delete(new Path(Filename));
        }


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

        // 3 下载文件
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
        if (!fs.exists(new Path(filename))){
            System.out.println("要下载的文件不存在");
        }else {
            fs.copyToLocalFile(false, new Path(filename), new Path(Filename), true);
        }

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

        // 3 重命名文件
        String filename ;
        Scanner scanner = new Scanner(System.in);
        System.out.println("请输入原文件名称：例如：/user/zoujiahao1905140016/demo-three/banzhang.txt");
        filename = scanner.next();
        String refilename ;
        Scanner scanner2 = new Scanner(System.in);
        System.out.println("请输入修改后文件名称：例如：/user/zoujiahao1905140016/demo-three/bangzhan.txt");
        refilename = scanner2.next();

        if (!fs.exists(new Path(filename))){
            System.out.println("要重命名的文件不存在");
        }else {
            fs.rename(new Path(filename), new Path(refilename));
        }


        // 4 关闭资源
        fs.close();
    }

//    //文件列出
//    public void Listfile() throws IOException, InterruptedException, URISyntaxException {
//
//        // 1 获取文件系统
//        Configuration configuration = new Configuration();
//
//        // 2 添加配置文件
//        Properties properties = new Properties();
//        properties.load(new FileInputStream("src/main/java/Hadoop/Hadoop.properties"));
//        String URI = properties.getProperty("URI");
//        String user = properties.getProperty("user");
//        FileSystem fs = FileSystem.get(new URI(URI), configuration, user);
//
//        // 3 列出文件
//        String filename ;
//        Scanner scanner = new Scanner(System.in);
//        System.out.println("请输入要查看的文件夹：例如：/user/");
//        filename = scanner.next();
//
//
//        RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(new Path(filename),true);
//        while (iterator.hasNext()){
//            LocatedFileStatus fileStatus = iterator.next();
//            Path fullpath = fileStatus.getPath();
//            System.out.println(fullpath);
//        }
//
//        // 4 关闭资源
//        fs.close();
//
//    }
//
//    public void ListFile() throws IOException, InterruptedException, URISyntaxException {
//
//        // 1 获取文件系统
//        Configuration configuration = new Configuration();
//
//        // 2 添加配置文件
//        Properties properties = new Properties();
//        properties.load(new FileInputStream("src/main/java/Hadoop/Hadoop.properties"));
//        String URI = properties.getProperty("URI");
//        String user = properties.getProperty("user");
//        FileSystem fs = FileSystem.get(new URI(URI), configuration, user);
//
//        // 3 列出文件和文件夹
//        String Filename;
//        Scanner scanner = new Scanner(System.in);
//        System.out.println("请输入要查看的文件夹：例如：/user/");
//        Filename = scanner.next();
//
//        FileStatus[] fileStatuses = fs.listStatus(new Path(Filename));
//        for (FileStatus fileStatus : fileStatuses){
//            if (fileStatus.isDirectory()){
//                boolean isDir = fileStatus.isDirectory();
//                String fullpath = fileStatus.getPath().toString();
//                System.out.println("文件" + isDir + ",名称" + fullpath);
//            }
//        }
//        fs.close();
//
//    }

    //列出所有文件和目录
    public void ListFiles() throws IOException, InterruptedException, URISyntaxException {

        // 1 获取文件系统
        Configuration configuration = new Configuration();

        // 2 添加配置文件
        Properties properties = new Properties();
        properties.load(new FileInputStream("src/main/java/Hadoop/Hadoop.properties"));
        String URI = properties.getProperty("URI");
        String user = properties.getProperty("user");
        FileSystem fs = FileSystem.get(new URI(URI), configuration, user);

        // 3 查看文件夹
        String Filenames ;
        Scanner scanner = new Scanner(System.in);
        System.out.println("请输入要查看的文件目录：例如：/user/");
        Filenames = scanner.next();
        if (!fs.exists(new Path(Filenames))){
            System.out.println("要查看的文件目录不存在");
        }else {
            FileStatus[] fileStatuses = fs.listStatus(new Path(Filenames));
            for (FileStatus fileStatus : fileStatuses){
                System.out.println(fileStatus.getPath().toString());
            }
            RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(new Path(Filenames),true);
            while (iterator.hasNext()){
                LocatedFileStatus fileStatus = iterator.next();
                Path fullpath = fileStatus.getPath();
                System.out.println(fullpath);
            }
        }

        fs.close();
    }

    //查看文件内容

    public void Showfile() throws IOException, InterruptedException, URISyntaxException {

        // 1 获取文件系统
        Configuration configuration = new Configuration();

        // 2 添加配置文件
        Properties properties = new Properties();
        properties.load(new FileInputStream("src/main/java/Hadoop/Hadoop.properties"));
        String URI = properties.getProperty("URI");
        String user = properties.getProperty("user");
        FileSystem fs = FileSystem.get(new URI(URI), configuration, user);

        // 3 显示文件内容
        String Filename;
        Scanner scanner = new Scanner(System.in);
        System.out.println("请输入要查看的文件名：例如：/user/zoujiahao1905140016/demo-three/banzhang.txt");
        Filename = scanner.next();
        if (!fs.exists(new Path(Filename))){
            System.out.println("要显示的文件不存在！");
        }else{
            FSDataInputStream  fsDataInputStream = fs.open(new Path(Filename));
            InputStreamReader inputStreamReader = new InputStreamReader(fsDataInputStream);
            BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

            String string = bufferedReader.readLine();
            while (string != null){
                System.out.println(string);
                string = bufferedReader.readLine();
            }
            bufferedReader.close();
            inputStreamReader.close();
            fsDataInputStream.close();
        }
        fs.close();
    }

}


