package MapReduce.EmployeeSalary;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


public class Part extends Partitioner<Text, Bean> {

    @Override
    public int getPartition(Text text, Bean bean , int i) {
        int salary = bean.getSalary();

        if (salary < 1500){
            return 1;
        } if ( salary < 3000){
            return 2;
        }if(salary > 4500){
            return 3;
        }
        else {
            return 0;
        }
    }
}
