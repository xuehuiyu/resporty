package com.bazhi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class SubmissionMR2 {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //创建MR 任务对象
        Configuration entries = new Configuration();
        Job job = Job.getInstance(entries, "wordcounnt");
        job.setJarByClass(SubmissionMR2.class);
        //设置数据的输入和输出类型
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        //设置数据来源和计算结果的输出位置
        TextInputFormat.addInputPath(job, new Path("file:///e:\\phone.txt"));
        TextOutputFormat.setOutputPath(job, new Path("file:///e:\\gfgf"));
        //指定MAP和redurcer 任务的实现类//file:///e:\\result
        job.setMapperClass(MyMapper.class);

        //设置Map和Reducer的输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        //提交任务
         job.waitForCompletion(true);
    }

}
