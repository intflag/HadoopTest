package com.intflag.mr.access;

import com.amazonaws.services.simplesystemsmanagement.model.DoesNotExistException;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author liugx  QQ:1598749808
 * @version V1.0
 * @date 2019-05-04 14:42
 * @Description
 */
public class AccessReducer extends Reducer<Text, Access, NullWritable, Access> {
    @Override
    protected void reduce(Text key, Iterable<Access> values, Context context) throws IOException, InterruptedException {
        Long ups = 0L;
        Long downs = 0L;
        for (Access value : values) {
            ups += value.getUp();
            downs += value.getDown();
        }
        context.write(NullWritable.get(), new Access(key.toString(), ups, downs));

    }
}
