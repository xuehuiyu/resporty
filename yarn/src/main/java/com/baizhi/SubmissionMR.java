package com.baizhi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


import java.io.IOException;

public class SubmissionMR {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //创建mr任务对象
        Configuration entries = new Configuration();
        Job wordcount = Job.getInstance(entries, "wordcount");
        wordcount.setJarByClass(SubmissionMR.class);
        //设置数据的输出类型和输入类型
        //基于文本文件的数据输入和输出
        wordcount.setInputFormatClass(TextInputFormat.class);
        wordcount.setOutputFormatClass(TextOutputFormat.class);
        //设置数据来源和计算结果的输出位置
        TextInputFormat.addInputPath(wordcount,new Path("/baizhi/word.txt"));
        //MR 程序的输出结果个位置必须不存在
        TextOutputFormat.setOutputPath(wordcount,new Path("/baizhi/result"));
        //指定MR程序的具体实现类
        wordcount.setMapperClass(MyMapper.class);
        wordcount.setReducerClass(MyReduce.class);
        //设置map任务和reduce任务的keyout|valueout类型
        wordcount.setMapOutputKeyClass(Text.class);
        wordcount.setMapOutputValueClass(IntWritable.class);
        wordcount.setOutputKeyClass( Text.class);
        wordcount.setOutputValueClass(IntWritable.class);
        //提交任务
        boolean b = wordcount.waitForCompletion(true);


    }
}
