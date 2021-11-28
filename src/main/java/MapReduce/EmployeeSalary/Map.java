package MapReduce.EmployeeSalary;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Map extends Mapper<LongWritable, Text, Text, Bean> {
    Text text = new Text();
    Bean bean = new Bean();

    public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException {
        String[] split = value.toString().split(",");
        String subject = split[1];
        String Ssalary = split[6];
        if (Ssalary.length() == 0 || Ssalary.length() == 1){
            split[6] = "0";
        }
        int salary_one = Integer.parseInt(split[5]);  //工资
        int salary_two = Integer.parseInt(split[6]);  //奖金
//        int salary = salary_one + salary_two;         //总工资


        bean.set(salary_one,salary_two);
        bean.setSalary_one(salary_one);
        bean.setSalary_two(salary_two);
        bean.setSalary(salary_one + salary_two);
//        bean.set(salary_one, salary_two);



        text.set(subject);
        context.write(text,bean);


    }


}
