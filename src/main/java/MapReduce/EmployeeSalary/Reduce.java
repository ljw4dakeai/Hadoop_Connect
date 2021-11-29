package MapReduce.EmployeeSalary;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Reduce extends Reducer<Text, Bean,Text, Bean> {
    Bean bean  = new Bean();
    public void reduce(Text key, Iterable<Bean> values, Context context) throws IOException, InterruptedException {
        int salary_one = 0;
        int salary_two = 0;

        for (Bean value : values){
            salary_one += value.getSalary_one();
            salary_two += value.getSalary_two();

        }

        bean.setSalary_one(salary_one);
        bean.setSalary_two(salary_two);
        bean.setSalary(salary_one + salary_two);


        context.write(key,bean);


    }
}
