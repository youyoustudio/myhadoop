package com.youyoustudio.myhadoop.weather;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MyWeatherSortComparator extends WritableComparator {

    public MyWeatherSortComparator(){
        super(Weather.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        Weather weatherA = (Weather) a;
        Weather weatherB = (Weather) b;
        int c1 = Integer.compare(weatherA.getYear(),weatherB.getYear());
        if(c1 == 0){
            int c2 = Integer.compare(weatherA.getMonth(),weatherB.getMonth());
            if(c2 == 0){
                return - Integer.compare(weatherA.getWeather(),weatherB.getWeather());
            }
            return c2;
        }
        return  c1;
    }
}
