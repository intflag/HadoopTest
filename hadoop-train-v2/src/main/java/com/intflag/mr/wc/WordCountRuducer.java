package com.intflag.mr.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author liugx  QQ:1598749808
 * @version V1.0
 * @date 2019-05-04 12:26
 * @Description
 */
public class WordCountRuducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        Iterator<IntWritable> iterator = values.iterator();
        while (iterator.hasNext()) {
            count += iterator.next().get();
        }
        context.write(key,new IntWritable(count));
    }
}
