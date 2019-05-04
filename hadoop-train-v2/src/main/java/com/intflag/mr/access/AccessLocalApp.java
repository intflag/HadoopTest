package com.intflag.mr.access;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author liugx  QQ:1598749808
 * @version V1.0
 * @date 2019-05-04 14:46
 * @Description
 */
public class AccessLocalApp {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        job.setJarByClass(AccessLocalApp.class);

        job.setMapperClass(AccessMapper.class);
        job.setReducerClass(AccessReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Access.class);

        //设置自定义分区规则
        job.setPartitionerClass(AccessPartitioner.class);
        //设置分区大小
        job.setNumReduceTasks(3);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Access.class);

        FileInputFormat.setInputPaths(job, new Path("hadoop-train-v2/access/input"));
        FileOutputFormat.setOutputPath(job, new Path("hadoop-train-v2/access/output"));

        boolean res = job.waitForCompletion(true);

        System.exit(res ? 0 : -1);


    }
}
