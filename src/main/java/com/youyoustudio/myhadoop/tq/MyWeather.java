package com.youyoustudio.myhadoop.tq;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

public class MyWeather {
    public static void main(String[] args) throws Exception{
        Configuration configuration = new Configuration(true);
        Job job = Job.getInstance(configuration);
        job.setJarByClass(MyWeather.class);

    }
}
