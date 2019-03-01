package com.baizhi;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MyMapper extends Mapper<LongWritable, Text,Text,MyFile> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String string = value.toString();
        String[] s = string.split(" ");
        String name = s[0];
        Long upload = Long.parseLong(s[1]);
        Long download = Long.parseLong(s[2]);
        context.write(new Text(name),new MyFile(upload,download));

    }
}
