package com.youyoustudio.myhadoop.fof;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FoFReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    Text mKey = new Text();
    IntWritable mValue = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        boolean isFriend = false;//是否为直接好友
        int count = 0;
        for (IntWritable v : values) {
            if (v.get() == 0) {//为直接关系，直接过滤
                isFriend = true;
                break;
            }
            count++;
        }
        if (isFriend == false) {
            mKey.set(key.toString());
            mValue.set(count);
            context.write(mKey, mValue);
        }
    }
}
