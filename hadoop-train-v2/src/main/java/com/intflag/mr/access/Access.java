package com.intflag.mr.access;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author liugx  QQ:1598749808
 * @version V1.0
 * @date 2019-05-04 14:31
 * @Description 日志对象
 */
public class Access implements Writable {

    private String phone;
    private Long up;
    private Long down;
    private Long sum;


    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(phone);
        out.writeLong(up);
        out.writeLong(down);
        out.writeLong(sum);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.phone = in.readUTF();
        this.up = in.readLong();
        this.down = in.readLong();
        this.sum = in.readLong();
    }

    public Access() {
    }

    public Access(String phone, Long up, Long down) {
        this.phone = phone;
        this.up = up;
        this.down = down;
        this.sum = up + down;
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public Long getUp() {
        return up;
    }

    public void setUp(Long up) {
        this.up = up;
    }

    public Long getDown() {
        return down;
    }

    public void setDown(Long down) {
        this.down = down;
    }

    public Long getSum() {
        return sum;
    }

    public void setSum(Long sum) {
        this.sum = sum;
    }

    @Override
    public String toString() {
        return "Access{" +
                "phone='" + phone + '\'' +
                ", up=" + up +
                ", down=" + down +
                ", sum=" + sum +
                '}';
    }
}
