package MapReduce.SecondSort;


import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Bean implements WritableComparable<Bean> {
    public int First;
    public int Second;

    public Bean(){
        super();
    }

    public Bean(int first, int second) {
        super();
        First = first;
        Second = second;
    }

    @Override
    public String toString() {
        return First + "\t" + Second;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(First);
        dataOutput.writeInt(Second);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        First = dataInput.readInt();
        Second = dataInput.readInt();
    }

    public int getFirst() {
        return First;
    }

    public void setFirst(int first) {
        First = first;
    }

    public int getSecond() {
        return Second;
    }

    public void setSecond(int second) {
        Second = second;
    }

    @Override
    public int compareTo(Bean o) {

        if (First != o.First)
            return First < o.First ? 1 : -1;
        else if (Second != o.Second)
            return Second < o.Second ? -1 : 1;
        else
            return 0;
    }

    @Override
    public int  hashCode(){

        return First * 13 + Second;
    }

    @Override
    public boolean equals(Object second){
        if (second == null)
            return false;
        if (this == second)
            return true;
        if (second instanceof Bean) {
            Bean r = (Bean) second;
            return r.First == First && r.Second == Second;
        }
        else
            return false;
    }

}
