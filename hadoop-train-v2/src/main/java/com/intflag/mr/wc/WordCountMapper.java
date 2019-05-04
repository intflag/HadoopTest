package com.intflag.mr.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author liugx  QQ:1598749808
 * @version V1.0
 * @date 2019-05-04 12:23
 * @Description
 */
public class WordCountMapper extends Mapper<LongWritable,Text,Text,IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = value.toString().split("\t");
        for (String word : words) {
            context.write(new Text(word),new IntWritable(1));
        }
    }
}
