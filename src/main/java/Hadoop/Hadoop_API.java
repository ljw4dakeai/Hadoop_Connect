package Hadoop;


import java.io.FileInputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Properties;
import java.util.Scanner;

public class Hadoop_API {
    public static void main(String[] args) throws IOException {
        Hadoop_Connect  H = new Hadoop_Connect();
        String key ;//接受用户输入
        Scanner scanner = new Scanner(System.in);
        boolean loop =true;
        while (loop){
            System.out.println("------------------------------------------------");
            System.out.println("      Mkdir      :创建文件夹");
            System.out.println("    Deletdir     :删除文件夹");
//            System.out.println("     Listfile    :列出文件");
//            System.out.println("     ListFile    :列出文件夹");
            System.out.println("    ListFiles    :列出文件夹");
            System.out.println("     Showfile    :查看文件内容");
            System.out.println("CopyFromLocalFile:上传文件");
            System.out.println(" CopyTOLocalFile :下载文件");
            System.out.println("      Copy       :复制文件");
            System.out.println("     Rename      :重命名文件");
            System.out.println("      exit       :退出");
            System.out.println("-----------------------------------------------");
            System.out.print("请输入英文：");
            key = scanner.next();//接受一个字符
            switch (key){
                case "Mkdir":
                    try {
                        H.Mkdirs();
                    } catch (InterruptedException | URISyntaxException e) {
                        e.printStackTrace();
                    }
                    break;

                case "Deletdir":
                    try {
                        H.Delect();
                    } catch (InterruptedException | URISyntaxException e) {
                        e.printStackTrace();
                    }
                    break;
                case "CopyFromLocalFile":
                    try {
                        H.CopyFromLocalFile();
                    } catch (InterruptedException | URISyntaxException e) {
                        e.printStackTrace();
                    }
                    break;

                case "CopyTOLocalFile":
                    try {
                        H.CopyToLocalFile();
                    } catch (InterruptedException | URISyntaxException e) {
                        e.printStackTrace();
                    }
                    break;

                case "Rename":
                    try {
                        H.Rename();
                    } catch (InterruptedException | URISyntaxException e) {
                        e.printStackTrace();
                    }
                    break;

//                case "Listfile":
//                    try {
//                        H.Listfile();
//                    } catch (InterruptedException | URISyntaxException e) {
//                        e.printStackTrace();
//                    }
//                    break;
//
//                case "ListFile":
//                    try {
//                        H.ListFile();
//                    } catch (InterruptedException | URISyntaxException e) {
//                        e.printStackTrace();
//                    }
//                    break;
                case "ListFiles":
                    try {
                        H.ListFiles();
                    } catch (InterruptedException | URISyntaxException e) {
                        e.printStackTrace();
                    }
                    break;

                case "exit":
                    scanner.close();
                    loop = false;
                    break;

                case "Showfile":
                    try {
                        H.Showfile();
                    } catch (InterruptedException | URISyntaxException e) {
                        e.printStackTrace();
                    }
                    break;
                case "Copy":
                    try {
                        H.Copy();
                    } catch (InterruptedException | URISyntaxException e) {
                        e.printStackTrace();
                    }
                    break;

                default:
                    break;
            }
        }
        System.out.println("程序退出");

    }
}
