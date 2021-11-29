package MapReduce.SecondSort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupingCompar extends WritableComparator
{
    // 这是一个比较器，需要继承WritableComparator
    protected GroupingCompar() {
        super(Bean.class, true);
    }
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {

        // 在reduce阶段，构造一个key对应的value迭代器的时候，只要first相同就属于同一个组，放在一个value迭代器
        Bean ip1 = (Bean) w1;
        Bean ip2 = (Bean) w2;
        int First = ip1.getFirst();
        int Second = ip2.getFirst();


        //比较click_num大小，相等返回0，小于返回-1，大于返回1
        return Integer.compare(First, Second);
    }
}

