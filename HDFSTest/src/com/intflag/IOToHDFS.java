package com.intflag;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author liugx  QQ:1598749808
 * @version V1.0
 * @date 2018-12-07 11:25
 * @Description IO流的方式操作HDFS
 */
public class IOToHDFS {

    /**
     * 获取文件系统
     */
    public FileSystem getFileSystem() {
        // 1 创建配置对象
        Configuration conf = new Configuration();
        // 2 获取文件系统
        FileSystem fs = null;
        try {
            fs = FileSystem.get(new URI("hdfs://hadoop102:8020"), conf, "intflag");

            return fs;
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 关闭资源
     *
     * @param fs
     */
    private static void closeFileSystem(FileSystem fs) {
        if (fs != null) {
            try {
                fs.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 文件上传到HDFS
     */
    @Test
    public void putFileToHDFS() {
        // 1 获取HDFS
        FileSystem fs = getFileSystem();

        try {
            // 2 获取输出流
            FSDataOutputStream fos = fs.create(new Path("/user/intflag/input/x3.000"));
            // 3 获取输入流
            FileInputStream fis = new FileInputStream(new File("D:/test/x3.000"));
            // 4 流对接
            IOUtils.copyBytes(fis, fos, new Configuration());
            // 5 关闭流
            IOUtils.closeStream(fis);
            IOUtils.closeStream(fos);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            // 5 关闭资源
            closeFileSystem(fs);
        }
    }

    /**
     * 下载文件
     */
    @Test
    public void getFileFromHDFS() {
        // 1 获取HDFS
        FileSystem fs = getFileSystem();

        try {
            // 2 获取输入流
            FSDataInputStream fis = fs.open(new Path("/user/intflag/input/liugx.txt"));

            // 3 获取输出流
            FileOutputStream fos = new FileOutputStream(new File("D:/test/liugx.txt"));

            // 4 流对接
            IOUtils.copyBytes(fis, fos, new Configuration());
            // 5 关闭流
            IOUtils.closeStream(fis);
            IOUtils.closeStream(fos);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            // 5 关闭资源
            closeFileSystem(fs);
        }
    }

    /**
     * 下载大文件-下载第一块
     */
    @Test
    public void getFileFromHDFSSeek1() {
        // 1 获取HDFS
        FileSystem fs = getFileSystem();

        try {
            // 2 获取输入流
            FSDataInputStream fis = fs.open(new Path("/user/intflag/input/hadoop-2.7.2.tar.gz"));

            // 3 获取输出流
            FileOutputStream fos = new FileOutputStream(new File("D:/test/hadoop-2.7.2.tar.gz.part1"));

            // 4 流对接（只读取128m）
            byte[] buff = new byte[1024];
            //1024 * 1024 * 128
            int len = 1024 * 128;
            for (int i = 0; i < len; i++) {
                fis.read(buff);
                fos.write(buff);
            }
            // 5 关闭流
            IOUtils.closeStream(fis);
            IOUtils.closeStream(fos);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            // 5 关闭资源
            closeFileSystem(fs);
        }
    }

    /**
     * 下载大文件-下载第二块
     */
    @Test
    public void getFileFromHDFSSeek2() {
        // 1 获取HDFS
        FileSystem fs = getFileSystem();

        try {
            // 2 获取输入流
            FSDataInputStream fis = fs.open(new Path("/user/intflag/input/hadoop-2.7.2.tar.gz"));

            // 3 获取输出流
            FileOutputStream fos = new FileOutputStream(new File("D:/test/hadoop-2.7.2.tar.gz.part2"));

            // 4 流对接（只读取128m）
            // 定位到128m
            int len = 1024 * 1024 * 128;
            fis.seek(len);
            IOUtils.copyBytes(fis, fos, new Configuration());

            // 5 关闭流
            IOUtils.closeStream(fis);
            IOUtils.closeStream(fos);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            // 5 关闭资源
            closeFileSystem(fs);
        }
    }
}
