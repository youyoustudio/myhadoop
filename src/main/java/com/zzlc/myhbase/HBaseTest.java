package com.zzlc.myhbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class HBaseTest {
    Admin admin;

    @Before
    public void init() throws IOException {
        Configuration configuration = new Configuration(true);
        Connection connection = ConnectionFactory.createConnection(configuration);
        admin = connection.getAdmin();
    }

    @Test
    public void createTableTest(){

    }
}
