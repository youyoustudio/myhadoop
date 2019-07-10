package com.zzlc.myhadoop.fof;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FoFMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    Text mKey = new Text();
    IntWritable mValue = new IntWritable();//直接关系为0，间接关系为1

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //tom hello hadoop cat
        String[] strs = StringUtils.split(value.toString(), ' ');
        for (int i = 1; i < strs.length; i++) {
            mKey.set(getFof(strs[0],strs[i]));
            mValue.set(0);
            context.write(mKey,mValue);
            for(int j = i + 1;j<strs.length;j++){
                mKey.set(getFof(strs[i],strs[j]));
                mValue.set(1);
                context.write(mKey,mValue);
            }
        }
    }

    private static String getFof(String s1, String s2) {
        if (s1.compareTo(s2) < 0) {
            return s1 + ":" + s2;
        }
        return s2 + ":" + s1;
    }
}
