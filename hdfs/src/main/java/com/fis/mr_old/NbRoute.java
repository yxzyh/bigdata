package com.fis.mr_old;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;


/**
 * @author Geoge
 * @date 2018/4/23 0023 下午 4:24
 */
public class NbRoute implements Tool {

    public static final Logger log = LoggerFactory.getLogger(NbRoute.class);
    Configuration configuration;

    public static class MyMap extends Mapper<LongWritable,Text,Text,Text>{

        public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
            String[] citation = value.toString().split(",");
            context.write(new Text(citation[0]),new Text(citation[1]));
        }
    }

    public static class MyReduce extends Reducer<Text,Text,Text,Text>{
        public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException {
            String cvs = "";
            for(Text val : values){
                if(cvs.length() > 0){
                    cvs +=",";
                }
                if(!cvs.contains(val.toString())){
                    cvs += val.toString();
                }
            }

            String[] tmp = cvs.split(",");
            Arrays.sort(tmp);
            String scenic = "";
            if(tmp.length > 2){
                for(int i = 0;i < tmp.length;i++){
                    scenic += tmp[i];
                    scenic += ",";
                }
                String sctmp = scenic.substring(0,scenic.length()-1);
                context.write(key,new Text(sctmp));
            }

        }
    }

    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf,"NbRoute");
        job.setJarByClass(NbRoute.class);
        job.setJobName("NbRoute");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        job.setMapperClass(MyMap.class);
        job.setCombinerClass(MyReduce.class);
        job.setReducerClass(MyReduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        boolean success = job.waitForCompletion(true);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return success ? 0 : 1;
    }

    public void setConf(Configuration conf) {
        conf = new Configuration();
        configuration = conf;
    }

    public Configuration getConf() {
        return configuration;
    }

    public static void main(String[] args) throws Exception {

        int ret = ToolRunner.run(new NbRoute(), args);
        System.exit(ret);
    }
}
