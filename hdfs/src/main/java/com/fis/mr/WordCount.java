package com.fis.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * @author Geoge
 * @date 2018/4/20 0020 下午 5:41
 */
public class WordCount {

    public static class Map extends MapReduceBase implements Mapper<LongWritable,Text,Text,IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        public void map(LongWritable longWritable, Text text, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {

            String line = text.toString();
            StringTokenizer stringTokenizer = new StringTokenizer(line);

            while (stringTokenizer.hasMoreTokens()){
                word.set(stringTokenizer.nextToken());
                outputCollector.collect(word,one);
            }


        }
    }
}
