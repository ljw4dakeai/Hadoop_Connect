# java配置链接Hadoop

## 1.Hadoop版本为3.2.2

自己版本不对应在pro.xml中修改对应版本文件的jar包

## 2.配置文件URI和user在Hadoop.properties中

使用前请修改

## 3.hafs-site.xml删除后导入自己的配置文件

## 4.设置jar的时候新建一个recourse，不要使用自带的recourse
修改项目逻辑以后，需要重新打包jar包
![image](https://z3.ax1x.com/2021/11/11/I0HEv9.png)
![image](https://z3.ax1x.com/2021/11/11/I0HZuR.png)
![image](https://z3.ax1x.com/2021/11/11/I0Hk34.png)

## 4.配置类的运行环境
![image](https://z3.ax1x.com/2021/11/11/I0HAgJ.png)
![image](https://z3.ax1x.com/2021/11/11/I0HeD1.png)
## 5.MapReduce,使用前需要再配置一个Hadoop到自己电脑上，环境变量要设置成自己的电脑的环境变量，然后配置自己的类的运行环境

###### 环境变量自需要配置recourse里面的五个（log4j不需要）
然后里面的文件夹根据haadoop解压目录来
我的自己hadoop电脑的配置环境（recourse里的是集群上的配置文件，里面的HAOOP_HOME,修改为自己的例如下面的）
![image](https://z3.ax1x.com/2021/11/11/I0bsJO.png)
![image](https://z3.ax1x.com/2021/11/11/I0bvT0.png)
不要直接用虚拟机里面的，就好了



