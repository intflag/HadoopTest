package com.intflag.mr.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

/**
 * @author liugx  QQ:1598749808
 * @version V1.0
 * @date 2019-05-04 12:31
 * @Description
 */
public class WordCountApp {

    public static void main(String[] args) throws Exception {

        System.setProperty("HADOOP_USER_NAME","intflag");

        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS","hdfs://192.168.25.10:8020");


        //创建一个job
        Job job = Job.getInstance(configuration);
        //设置主类的参数
        job.setJarByClass(WordCountApp.class);

        //设置自定义的Mapper和Reducer
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountRuducer.class);

        //设置Combiner
        job.setCombinerClass(WordCountRuducer.class);

        //设置Mapper的输入和输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //设置Reducer的输入和输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //判断输出路径是否已经存在
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.25.10:8020"), configuration, "intflag");
        Path outputPath = new Path("/wordcount/output/");
        if (fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath,true);
        }

        //设置输入和输出的作业，以及文件文件位置
        FileInputFormat.setInputPaths(job, new Path("/wordcount/input/"));
        FileOutputFormat.setOutputPath(job, outputPath);

        //提交job
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : -1);

    }
}
