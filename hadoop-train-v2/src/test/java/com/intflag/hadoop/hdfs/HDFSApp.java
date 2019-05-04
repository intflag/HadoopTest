package com.intflag.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.net.URI;

/**
 * @author liugx  QQ:1598749808
 * @version V1.0
 * @date 2019-05-02 20:38
 * @Description
 */
public class HDFSApp {

    @Test
    public void fun1() throws Exception {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.25.10:8020"), configuration, "intflag");
        boolean b = fileSystem.mkdirs(new Path("/api/test"));
        System.out.println(b);
    }
}
