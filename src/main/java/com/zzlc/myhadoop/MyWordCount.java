package com.zzlc.myhadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyWordCount {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration(true);
        Job job = Job.getInstance(configuration);
        job.setJarByClass(MyWordCount.class);
        job.setJobName("myjob");

        //设置输入源和输出源
        Path pathIn = new Path("/test/hello.txt");
        Path pathOut = new Path("/aabb/ccdd/");
        FileSystem fileSystem = pathOut.getFileSystem(configuration);
        if(fileSystem.exists(pathOut)){
            fileSystem.delete(pathOut,true);
        }
        FileInputFormat.addInputPath(job, pathIn);
        FileOutputFormat.setOutputPath(job, pathOut);

        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(MyReducer.class);
        job.waitForCompletion(true);
    }
}
