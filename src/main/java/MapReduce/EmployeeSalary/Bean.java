package MapReduce.EmployeeSalary;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Bean implements Writable {
    public int salary_one;
    public int salary_two;
    public int salary;

    public Bean(){
        super();
    }

    public Bean(int salary_one, int salary_two){
        super();
        this.salary_one = salary_one;
        this.salary_two = salary_two;
        this.salary = salary_one + salary_two;

    }

    //序列化
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(salary_one);
        dataOutput.writeInt(salary_two);
        dataOutput.writeInt(salary);
    }

    //反序列化
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        salary_one = dataInput.readInt();
        salary_two = dataInput.readInt();
        salary = dataInput.readInt();
    }

    @Override
    public String toString() {
        return  salary_one + "\t" + salary_two + "\t" + salary;
    }

    public int getSalary_one() {
        return salary_one;
    }

    public void setSalary_one(int salary_one) {
        this.salary_one = salary_one;
    }

    public int getSalary_two() {
        return salary_two;
    }

    public void setSalary_two(int salary_two) {
        this.salary_two = salary_two;
    }

    public int getSalary() {
        return salary;
    }

    public void setSalary(int salary) {
        this.salary = salary;
    }

    public void set(int salary_one2,int salary_two2){
        this.salary_one = salary_one2;
        this.salary_two = salary_two2;
        this.salary = salary_one2 + salary_two2;
    }
}
