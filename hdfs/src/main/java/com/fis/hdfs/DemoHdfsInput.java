package com.fis.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;

import static org.apache.hadoop.security.UserGroupInformation.isSecurityEnabled;

/**
 * @author Geoge
 * @date 2018/4/20 0020 上午 11:31
 */
public class DemoHdfsInput {
    public static void main(String[] args) throws URISyntaxException, IOException {

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://dn2fis01:9000"),conf);
       // FSDataOutputStream out = fs.create(new Path("/friends11"));
        //FileInputStream in = new FileInputStream("d:/friends.txt"); //上传的文件
       // IOUtils.copyBytes(in, out, 1024, true);
        fs.copyFromLocalFile(new Path("d:/friends.txt"),new Path("/frends12"));
    }
}
