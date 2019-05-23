package com.youyoustudio.myhadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;

public class TestHDFS {
    Configuration conf;
    FileSystem fileSystem;

    @Before
    public void conn() throws IOException {
        conf = new Configuration(true);
        fileSystem = FileSystem.get(conf);
    }

    @After
    public void close() throws IOException {
        fileSystem.close();
    }

    @Test
    public void mkdir() throws IOException {
        Path path = new Path("/ooxx");
        if (fileSystem.exists(path)) {
            fileSystem.delete(path, true);
        }
        fileSystem.mkdirs(path);
    }

    @Test
    public void upload()throws IOException{
        Path path = new Path("/ooxx/hello.txt");
        FSDataOutputStream fsDataOutputStream = fileSystem.create(path);
        InputStream inputStream =
                new BufferedInputStream(new FileInputStream(new File("E:\\hello.txt")));
        IOUtils.copyBytes(inputStream,fsDataOutputStream,conf,true);
    }

    @Test
    public void download() throws IOException{
        Path path = new Path("/ooxx/hello.txt");
        FSDataInputStream fsDataInputStream = fileSystem.open(path);
        FileOutputStream fileOutputStream = new FileOutputStream(new File("E:\\aaa.txt"));
        IOUtils.copyBytes(fsDataInputStream,fileOutputStream,conf,true);
    }

    @Test
    public void blocks() throws IOException{
        Path path = new Path("/ooxx/hello.txt");
        FileStatus fileStatus = fileSystem.getFileStatus(path);
        BlockLocation[] fileBlockLocations = fileSystem.getFileBlockLocations(path, 0, fileStatus.getLen());
        for (BlockLocation fileBlockLocation : fileBlockLocations) {
            System.out.println(fileBlockLocation);

        }
    }
}
