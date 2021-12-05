package MapReduce.EmpDept.DeptEmp;

import MapReduce.EmpDept.DeptEmpJoin.Bean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class Reduce extends Reducer<Text, Bean,Text, Text> {
    Text text = new Text();

    public void reduce(Text key, Iterable<Bean> values, Context context) throws IOException, InterruptedException {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("YYYY/MM/DD");
        int content = 0;
        double salary = 0;
        double avg = 0;
        String data = "2000/10/10";
        int salary_persion = 0;
        String name = "";


        for (Bean value: values) {
            try {
                if(simpleDateFormat.parse(value.getEmpData()).compareTo(simpleDateFormat.parse(data)) < 0){
                    data = value.getEmpData();
                    name = value.getEmpName();
                    salary_persion = value.getEmpSalay();
                }
            } catch (ParseException e) {
                e.printStackTrace();
            }
            salary += value.getEmpSalay();
            content ++;

            avg = salary / content * 1f;

        }

        String out  = "人数：" + content + "\t" + "总工资：" + salary + "\t" + "平均工资：" + avg + "\t" +
                "本部门年纪年纪最大的人:" + name + "\t" + "他来的日期:" + data + "\t" + "他的工资：" + salary_persion ;
        text.set(out);

        context.write(key, text);

    }
}
