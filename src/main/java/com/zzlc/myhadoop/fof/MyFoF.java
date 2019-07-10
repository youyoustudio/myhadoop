package com.zzlc.myhadoop.fof;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MyFoF {
    public static void main(String[] args) throws Exception{
        Configuration configuration = new Configuration(true);
        Job job = Job.getInstance(configuration);
        job.setJobName("MyFoF");

        job.setJarByClass(MyFoF.class);
        //mapper
        job.setMapperClass(FoFMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //reduce
        job.setReducerClass(FoFReduce.class);
        //输入输出
        Path input = new Path("/data/fof/input");
        FileInputFormat.addInputPath(job,input);
        Path output = new Path("/data/fof/output");
        FileSystem fs = output.getFileSystem(configuration);
        if(fs.exists(output)){
            fs.delete(output,true);
        }
        FileOutputFormat.setOutputPath(job,output);

        job.waitForCompletion(true);
    }
}
