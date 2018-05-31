package com.fis.mr;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;


/**
 * @author Geoge
 * @date 2018/4/20 0020 下午 2:21
 */
public class DemoMR {
    public static void main(String[] args) throws IOException {
        //输入路径
        String dstIn="hdfs://dn2fis01:9000/aa.txt";
        //输出路径
        String dstOut="hdfs://dn2fis01:9000/out0420";
//        Configuration conf = new Configuration();
//        conf.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
//        conf.set("fs.file.impl",org.apache.hadoop.fs.LocalFileSystem.class.getName());
//        JobConf jobconf = new JobConf();
//        Job job = new Job(jobconf);

         JobConf conf = new JobConf(WordCount.class);
         conf.setJobName("wordcount");


         conf.setOutputKeyClass(Text.class);
         conf.setOutputValueClass(IntWritable.class);

         conf.setMapperClass(WordCount.Map.class);
         conf.setCombinerClass(Reduce.class);
         conf.setReducerClass(Reduce.class);

         conf.setInputFormat(TextInputFormat.class);
         conf.setOutputFormat(TextOutputFormat.class);

         FileInputFormat.setInputPaths(conf,new Path(dstIn));
         FileOutputFormat.setOutputPath(conf,new Path(dstOut));

         JobClient.runJob(conf);



    }
}
