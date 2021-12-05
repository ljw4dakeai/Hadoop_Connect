
package MapReduce.EmpDept.DeptEmpJoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

public  class Reduce extends Reducer<Text, Bean, Bean, NullWritable> {

    public void reduce(Text key, Iterable<Bean> values, Context context) throws IOException, InterruptedException {
        ArrayList<Bean> empbeans =  new ArrayList<>();//实现表的join
        Bean dept = new Bean();

        for (Bean value : values) {
            if ("emp".equals(value.getFlage())) {
                Bean bean = new Bean();
                try {
                    BeanUtils.copyProperties(bean, value);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
                empbeans.add(bean);
            } else {
                try {
                    BeanUtils.copyProperties(dept, value);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            }
        }

        for (Bean emp: empbeans){
            emp.setDeptName(dept.getDeptName());
            context.write(emp,NullWritable.get());
        }
    }

}
