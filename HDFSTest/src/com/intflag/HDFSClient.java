package com.intflag;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

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
        closeFileSystem(fileSystem);
    }
}

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

    @Test
    public void putFileToHDFS() {
        // 1 获取文件系统
        FileSystem fs = getFileSystem();
        // 2 上传文件
        try {
            fs.copyFromLocalFile(true, new Path("D:/test/x3.000"), new Path("/user/intflag/input/x3.000"));
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            // 3 关闭资源
            closeFileSystem(fs);
        }
    }

    /**
     * 从HDFS上下载文件
     */
    @Test
    public void getFileFromHDFS() {
        // 1 获取文件系统
        FileSystem fs = getFileSystem();
        // 2 上传文件
        try {
            fs.copyToLocalFile(new Path("/user/intflag/input/x3.000"), new Path("D:/test/x3.000"));
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            // 3 关闭资源
            closeFileSystem(fs);
        }
    }

    /**
     * 在HDFS上创建文件夹
     */
    @Test
    public void mkdirAtHDFS() {
        // 1 获取文件系统
        FileSystem fs = getFileSystem();
        // 2 创建文件夹
        try {
            fs.mkdirs(new Path("/user/intflag/test"));
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            // 3 关闭资源
            closeFileSystem(fs);
        }
    }

    /**
     * 删除HDFS上的文件夹
     */
    @Test
    public void deleteDirAtHDFS() {
        // 1 获取文件系统
        FileSystem fs = getFileSystem();
        // 2 删除文件夹
        try {
            fs.delete(new Path("/user/intflag/test"), true);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            // 3 关闭资源
            closeFileSystem(fs);
        }
    }

    /**
     * 重命名HDFS上的文件夹
     */
    @Test
    public void renameDirAtHDFS() {
        // 1 获取文件系统
        FileSystem fs = getFileSystem();
        // 2 重命名文件夹
        try {
            fs.rename(new Path("/user/intflag/test"), new Path("/user/intflag/test22"));
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            // 3 关闭资源
            closeFileSystem(fs);
        }
    }

    /**
     * 读取HDFS上的文件信息
     */
    @Test
    public void readFileAtHDFS() {
        // 1 获取文件系统
        FileSystem fs = getFileSystem();
        // 2 读取文件信息
        try {
            RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
            while (listFiles.hasNext()) {
                LocatedFileStatus status = listFiles.next();
                System.out.println("-----------------------------------");
                System.out.println("文件名称：" + status.getPath().getName());
                System.out.println("块的大小：" + status.getBlockSize());
                System.out.println("内容长度：" + status.getLen());
                System.out.println("文件权限：" + status.getPermission());
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            // 3 关闭资源
            closeFileSystem(fs);
        }
    }

    /**
     * 读取文件夹信息
     */
    @Test
    public void readFfolderAtHDFS() {
        // 1 获取文件系统
        FileSystem fs = getFileSystem();
        // 2 读取文件夹信息
        try {
            FileStatus[] listStatus = fs.listStatus(new Path("/user/intflag/"));
            for (FileStatus status : listStatus) {
                if (status.isFile()) {
                    System.out.println("f----" + status.getPath().getName());
                } else {
                    System.out.println("d----" + status.getPath().getName());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            // 3 关闭资源
            closeFileSystem(fs);
        }
    }


}
