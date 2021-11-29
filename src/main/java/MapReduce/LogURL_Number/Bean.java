package MapReduce.LogURL_Number;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Bean implements Writable {
    public String ID;
    public int Num;
    public int List;
    public String Url;

    public Bean(){super();}

    public Bean(String ID, int num, int list, String url) {
        ID = ID;
        Num = num;
        List = list;
        Url = url;
    }

    @Override
    public String toString() {
        return ID + "\t" + Num + "\t" + List + "\t" + Url;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(ID);
        dataOutput.writeInt(Num);
        dataOutput.writeInt(List);
        dataOutput.writeUTF(Url);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        ID = dataInput.readUTF();
        Num = dataInput.readInt();
        List = dataInput.readInt();
        Url = dataInput.readUTF();

    }

    public String getID() {
        return ID;
    }

    public void setID(String ID) {
        this.ID = ID;
    }

    public int getNum() {
        return Num;
    }

    public void setNum(int num) {
        Num = num;
    }

    public int getList() {
        return List;
    }

    public void setList(int list) {
        List = list;
    }

    public String getUrl() {
        return Url;
    }

    public void setUrl(String url) {
        Url = url;
    }
}
