package com.fis.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author Geoge
 * @date 2018/4/19 0019 下午 3:38
 */
public class DemoHdfs {
     static FileSystem fs;
    @Before
    public void init() throws URISyntaxException, IOException {
         fs = FileSystem.get(new URI("hdfs://dn2fis01:9000"),new Configuration());
    }

    @Test
    public void testUpLoad() throws IOException {
        //inputStream
        InputStream in = new FileInputStream("d:/storm1.2_out22");
        //outputStream
        OutputStream out = fs.create(new Path("/storm1.2_in22"));
        //IOUtils.CopyBytes
        IOUtils.copyBytes(in,out,4096,true);
    }

    @Test
    public void testDownLoad() throws IOException {
        fs.copyToLocalFile(new Path("/storm1.2"),new Path("d:/storm1.2_out24"));
    }

    @Test
    public void testDel() throws IOException {
        fs.deleteOnExit(new Path("/install.log"));
    }

    @Test
    public void testMkdir() throws IOException {
        boolean b = fs.mkdirs(new Path("/testMkdir3"));
        System.out.println(b);
    }

    public static void main(String[] args) throws URISyntaxException, IOException {
        FileSystem fs = FileSystem.get(new URI("hdfs://dn2fis01:9000"),new Configuration());
        InputStream in = fs.open(new Path("/aa.txt"));
        OutputStream out = new FileOutputStream("d:/data23222.txt");
        IOUtils.copyBytes(in,out,4096,true);

    }
}
