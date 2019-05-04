package com.intflag.mr.access;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.io.ObjectInput;

/**
 * @author liugx  QQ:1598749808
 * @version V1.0
 * @date 2019-05-04 14:36
 * @Description
 */
public class AccessMapper extends Mapper<LongWritable,Text,Text,Access> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] lines = value.toString().split("\t");

        String phone = lines[1];

        Long up = Long.valueOf(lines[lines.length-3]);
        Long down = Long.valueOf(lines[lines.length-2]);

        context.write(new Text(phone),new Access(phone,up,down));
    }
}
