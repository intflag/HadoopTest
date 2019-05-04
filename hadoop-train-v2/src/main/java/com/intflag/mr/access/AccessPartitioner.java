package com.intflag.mr.access;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author liugx  QQ:1598749808
 * @version V1.0
 * @date 2019-05-04 15:14
 * @Description
 */
public class AccessPartitioner extends Partitioner<Text, Access> {
    @Override
    public int getPartition(Text phone, Access access, int numReduceTasks) {
        if (phone.toString().startsWith("13")) {
            return 0;
        } else if (phone.toString().startsWith("15")) {
            return 1;
        } else {
            return 2;
        }
    }
}
