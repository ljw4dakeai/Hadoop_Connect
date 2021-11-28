package MapReduce.GrossProceeds;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Bean implements Writable {
    public int ID;
    public int num;
    public float Total;

    public Bean(){
        super();
    }

    public Bean(int ID, int num, float total) {
        this.ID = ID;
        this.num = num;
        this.Total = total;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(ID);
        dataOutput.writeInt(num);
        dataOutput.writeFloat(Total);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        ID = dataInput.readInt();
        num = dataInput.readInt();
        Total = dataInput.readFloat();
    }

    @Override
    public String toString() {
        return ID + "\t" + num + "\t" + Total;
    }

    public int getID() {
        return ID;
    }

    public void setID(int ID) {
        this.ID = ID;
    }

    public int getNum() {
        return num;
    }

    public void setNum(int num) {
        this.num = num;
    }

    public float getTotal() {
        return Total;
    }

    public void setTotal(float total) {
        this.Total = total;
    }
}


