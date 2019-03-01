package com.baizhi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.net.URISyntaxException;

public class SubmissionMR5Line {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        //创建MR 任务对象
        Configuration configuration = new Configuration();
        //设置行数据格式的默认几行是一个数据切片
        configuration.set("mapreduce.input.lineinputformat.linespermap","4");
        Job job = Job.getInstance(configuration,"work");
        job.setJarByClass(SubmissionMR5Line.class);
        //设置数据的输入和输出类型
        job.setInputFormatClass(NLineInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        //设置数据来源和计算结果的输出位置
        // 本地模拟二(本地环境+远程数据) 使用时运行Main行数即可
        //Nline 数据格式是把几行数据看作一个数据切片
         NLineInputFormat.addInputPath(job,new Path("file:///e:\\phone.txt"));
         TextOutputFormat.setOutputPath(job,new Path("file:///e:\\ddd"));
        //指定MAP和redurcer 任务的实现类//file:///e:\\result
         job.setMapperClass(MyMapper.class);
         job.setReducerClass(MyReducer.class);
        //设置Map和Reducer的输出类型
         job.setMapOutputValueClass(MyFile.class);
         job.setMapOutputKeyClass(Text.class);
         job.setOutputKeyClass(Text.class);
         job.setOutputValueClass(MyFile.class);
        //提交任务
       job.waitForCompletion(true);
    }

}
