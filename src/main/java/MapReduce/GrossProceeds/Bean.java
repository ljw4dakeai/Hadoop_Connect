package MapReduce.GrossProceeds;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Bean implements Writable {
    public int ID;
    public int Year;
    public float Total;

    public Bean(){
        super();
    }

    public Bean(int ID, int year, float total) {
        super();
        this.ID = ID;
        Year = year;
        Total = total;
    }

    //序列化
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(ID);
        dataOutput.writeInt(Year);
        dataOutput.writeFloat(Total);
    }

    //反序列化
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        ID= dataInput.readInt();
        Year = dataInput.readInt();
        Total = dataInput.readInt();
    }

    @Override
    public String toString() {
        return  ID + "\t" + Year + "\t" + Total;
    }

    public int getID() {
        return ID;
    }

    public void setID(int ID) {
        this.ID = ID;
    }

    public int getYear() {
        return Year;
    }

    public void setYear(int year) {
        Year = year;
    }

    public float getTotal() {
        return Total;
    }

    public void setTotal(float total) {
        Total = total;
    }
}
