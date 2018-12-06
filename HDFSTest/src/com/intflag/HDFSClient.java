package com.intflag;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * @author liugx  QQ:1598749808
 * @version V1.0
 * @date 2018-12-06 16:33
 * @Description HDFS客户端
 */
public class HDFSClient {

    public static void main(String[] args) {
        //1 加载配置
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://hadoop102:8020");

        FileSystem fileSystem = null;
        try {
            //2 获取文件系统
            fileSystem = FileSystem.get(configuration);

            //3 上传文件到HDFS指定位置
            fileSystem.copyFromLocalFile(new Path("D:/test/asd.txt"), new Path("/user/intflag/input/asd.txt"));
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (fileSystem != null) {
                try {
                    fileSystem.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
