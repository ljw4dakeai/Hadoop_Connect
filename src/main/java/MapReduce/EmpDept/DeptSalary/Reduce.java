package MapReduce.EmpDept.DeptSalary;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public  class Reduce extends Reducer<Text, Bean,Text, Text> {
    Text text = new Text();
    public void reduce(Text key, Iterable<Bean> values, Context context) throws IOException, InterruptedException {
        double salary = 0;
        int count = 0;
        double salary_avg = 0;
        for (Bean vlue:values){
            salary += vlue.getEmpSalay();
            count ++;
            salary_avg = salary / count*1f;

        }
        String out = "总工资：" + salary + "\t" + "人数：" + count + "\t" + "平均工资：" + salary_avg;
        text.set(out);
        context.write(key,text);
    }

}
