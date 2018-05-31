package com.fis.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author Geoge
 * @date 2018/4/20 0020 下午 5:49
 */
public class Reduce extends MapReduceBase implements Reducer<Text,IntWritable,Text,IntWritable> {
    public void reduce(Text text, Iterator<IntWritable> iterator, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
        int sum = 0;
        while (iterator.hasNext()){
            sum += iterator.next().get();
        }
        outputCollector.collect(text,new IntWritable(sum));
    }
}
