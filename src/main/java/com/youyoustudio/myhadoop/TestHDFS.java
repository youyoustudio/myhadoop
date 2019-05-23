package com.youyoustudio.myhadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class TestHDFS {
    Configuration conf;
    FileSystem fileSystem;

    @Before
    public void conn() throws IOException {
        conf = new Configuration(true);
        fileSystem = FileSystem.get(conf);
    }

    @After
    public void close() throws IOException{
        fileSystem.close();
    }

    @Test
    public void mkdir()throws IOException{

        try {
            Path path = new Path("/user");
            if (fileSystem.exists(path)) {
                fileSystem.delete(path, true);
            }
            //fileSystem.mkdirs(path);
        }catch (IOException e){
            e.printStackTrace();
        }
    }
}
