package com.zzlc.myhadoop.weather;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyWeather {
    /*public static void main(String[] args) throws Exception{
        //三大步 客户端，mapper，reduce
        Configuration configuration = new Configuration(true);
        Job job = Job.getInstance(configuration);
        job.setJarByClass(MyWeather.class);
        job.setJobName("MyWeatherJob");


        //mapper

        job.setMapperClass(MyWeatherMapper.class);//设置mapper
        job.setMapOutputKeyClass(Weather.class);//设置mapper输出key
        job.setMapOutputValueClass(IntWritable.class);//设置mapper输出value

        //partition
        job.setPartitionerClass(MyWeatherPartitioner.class);

        //排序比较器
        job.setSortComparatorClass(MyWeatherSortComparator.class);
        // map END


        //reduce
        //分组比较器
        job.setGroupingComparatorClass(MyWeatherGroupingComparator.class);
        job.setReducerClass(MyWeatherReduce.class);
        //reduce 结束

        //设置输入输出
        Path input = new Path("/data/weather/input");
        FileInputFormat.addInputPath(job,input);

        Path output = new Path("/data/weather/output");
        FileSystem fileSystem = output.getFileSystem(configuration);
        if(fileSystem.exists(output)){
            fileSystem.delete(output,true);
        }
        FileOutputFormat.setOutputPath(job,output);
        job.setNumReduceTasks(2);//设置reduce数量
        job.waitForCompletion(true);
    }*/
}
