package Hadoop;

import java.util.Scanner;

public class test {
    public static void main(String[] args) {
        String Filename ;
        Scanner scanner = new Scanner(System.in);
        System.out.println("请输入你要创建的文件夹名称：例如：/user/zoujiahao1905140016/demo-three");
        Filename = scanner.next();
        System.out.println(Filename);
    }
}
