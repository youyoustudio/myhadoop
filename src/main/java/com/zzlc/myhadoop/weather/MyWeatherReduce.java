package com.zzlc.myhadoop.weather;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MyWeatherReduce extends Reducer<Weather, IntWritable, Text,IntWritable> {

    Text rKey = new Text();
    IntWritable rval = new IntWritable();
    @Override
    protected void reduce(Weather key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //相同的key为一组
        // 2019 01 02  2 2
        // 2019 02 02  3 3
        int count = 0;
        for(IntWritable v : values){
            if(count <2) {
                String tmp = key.getYear() + "_" + key.getMonth() + "_"
                        + key.getDay() + ":" + key.getWeather();
                rKey.set(tmp);
                rval.set(key.getWeather());
                context.write(rKey, rval);
                count++;
            }else {
                break;
            }
        }

    }
}
