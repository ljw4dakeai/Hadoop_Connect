package MapReduce.EmpDept.DeptEmp;

import MapReduce.EmpDept.DeptEmpJoin.Bean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Map extends Mapper<LongWritable, Text, Text, Bean> {
    Bean bean = new Bean();
    Text text = new Text();

    public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");

        String EmpName = split[3];
        String EmpData = split[4];
        int EmpSalary_one = Integer.parseInt(split[5]);
        int EmpSalary_two = Integer.parseInt(split[6]);
        int salary = EmpSalary_one + EmpSalary_two;

        String Deptname = split[2];
        String Deptnum = split[1];

        bean.setDeptName(Deptname);
        bean.setDeptNum(Deptnum);
        bean.setEmpName(EmpName);
        bean.setEmpData(EmpData);
        bean.setFlage("");
        bean.setEmpSalay(salary);
        bean.setEmpSalay_one(EmpSalary_two);
        bean.setEmpSalay_two(EmpSalary_two);

        text.set(Deptname);

        context.write(text, bean);


    }


}
