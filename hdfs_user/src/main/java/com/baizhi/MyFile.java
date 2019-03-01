package com.baizhi;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MyFile implements Writable {
    private Long upload;
    private Long download;
    private Long sumload;

    @Override
    public String toString() {
        return "MyFile{" +
                "upload=" + upload +
                ", download=" + download +
                ", sumload=" + sumload +
                '}';
    }

    public Long getUpload() {
        return upload;
    }

    public void setUpload(Long upload) {
        this.upload = upload;
    }

    public Long getDownload() {
        return download;
    }

    public void setDownload(Long download) {
        this.download = download;
    }

    public Long getSumload() {
        return sumload;
    }

    public void setSumload(Long sumload) {
        this.sumload = sumload;
    }

    public MyFile() {
    }

    public MyFile(Long upload, Long download) {
        this.upload = upload;
        this.download = download;
        this.sumload = upload + download;
    }

    /**
     * 写方法
     *
     * @param out
     * @throws IOException
     */
    public void write(DataOutput out) throws IOException {
        out.writeLong(this.upload);
        out.writeLong(this.download);
        out.writeLong(this.sumload);
    }

    /**
     * 读方法
     *
     * @param in
     * @throws IOException
     */
    public void readFields(DataInput in) throws IOException {
        this.upload = in.readLong();
        this.download = in.readLong();
        this.sumload = in.readLong();

    }
}
