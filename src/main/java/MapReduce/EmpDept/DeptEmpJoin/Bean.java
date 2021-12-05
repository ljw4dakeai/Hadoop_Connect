package MapReduce.EmpDept.DeptEmpJoin;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public  class Bean implements Writable {
    public String EmpName  = " ";
    public String EmpData = " ";
    public int EmpSalay_one = 0;
    public int EmpSalay_two = 0;
    public int EmpSalay = 0;
    public String DeptNum = " ";
    public String DeptName = " ";
    public String flage = "";

    public Bean() {
        super();
    }

    public Bean(String empName, String empData, int empSalay_one, int empSalay_two, int empSalay, String deptNum, String deptName, String flage) {
        super();
        EmpName = empName;
        EmpData = empData;
        EmpSalay_one = empSalay_one;
        EmpSalay_two = empSalay_two;
        EmpSalay = empSalay;
        DeptNum = deptNum;
        DeptName = deptName;
        this.flage = flage;
    }

    @Override
    public String toString() {
        return flage + "\t" + DeptNum + "\t" + DeptName + "\t" + EmpName + "\t" + EmpData + "\t" +EmpSalay_one+ "\t" + EmpSalay_two + "\t" + EmpSalay;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(EmpName);
        dataOutput.writeUTF(EmpData);
        dataOutput.writeInt(EmpSalay_one);
        dataOutput.writeInt(EmpSalay_two);
        dataOutput.writeInt(EmpSalay);
        dataOutput.writeUTF(DeptNum);
        dataOutput.writeUTF(DeptName);
        dataOutput.writeUTF(flage);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        EmpName = dataInput.readUTF();
        EmpData = dataInput.readUTF();
        EmpSalay_one = dataInput.readInt();
        EmpSalay_two = dataInput.readInt();
        EmpSalay = dataInput.readInt();
        DeptNum = dataInput.readUTF();
        DeptName = dataInput.readUTF();
        flage = dataInput.readUTF();

    }

    public String getEmpName() {
        return EmpName;
    }

    public void setEmpName(String empName) {
        EmpName = empName;
    }

    public String getEmpData() {
        return EmpData;
    }

    public void setEmpData(String empData) {
        EmpData = empData;
    }

    public int getEmpSalay_one() {
        return EmpSalay_one;
    }

    public void setEmpSalay_one(int empSalay_one) {
        EmpSalay_one = empSalay_one;
    }

    public int getEmpSalay_two() {
        return EmpSalay_two;
    }

    public void setEmpSalay_two(int empSalay_two) {
        EmpSalay_two = empSalay_two;
    }

    public int getEmpSalay() {
        return EmpSalay;
    }

    public void setEmpSalay(int empSalay) {
        EmpSalay = empSalay;
    }

    public String getDeptNum() {
        return DeptNum;
    }

    public void setDeptNum(String deptNum) {
        DeptNum = deptNum;
    }

    public String getDeptName() {
        return DeptName;
    }

    public void setDeptName(String deptName) {
        DeptName = deptName;
    }

    public String getFlage() {
        return flage;
    }

    public void setFlage(String flage) {
        this.flage = flage;
    }
}
