package com.baizhi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class SubmissionMR {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //创建MR 任务对象
        Configuration entries = new Configuration();
        Job job = Job.getInstance(entries,"MyFile");
        job.setJarByClass(SubmissionMR.class);
        //设置数据的输入和输出类型
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        //设置数据来源和计算结果的输出位置
        TextInputFormat.addInputPath(job,new Path("/baizhi/phone.log"));
        TextOutputFormat.setOutputPath(job,new Path("/baizhi/result2"));
        //指定MAP和redurcer 任务的实现类
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        //设置Map和Reducer的输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MyFile.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MyFile.class);
        //提交任务
        job.waitForCompletion(true);
    }

}
