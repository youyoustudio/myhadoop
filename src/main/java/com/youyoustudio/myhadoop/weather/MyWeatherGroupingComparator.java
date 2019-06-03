package com.youyoustudio.myhadoop.weather;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MyWeatherGroupingComparator extends WritableComparator {
    public MyWeatherGroupingComparator(){
        super(Weather.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        Weather weatherA = (Weather) a;
        Weather weatherB = (Weather) b;
        int c1 = Integer.compare(weatherA.getYear(),weatherB.getYear());
        if(c1 == 0){
            return Integer.compare(weatherA.getMonth(),weatherB.getMonth());
        }
        return  c1;
    }
}
