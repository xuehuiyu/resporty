package com.baizhi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.net.URISyntaxException;


public class SubmissionMR6Combine {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        //创建MR 任务对象
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "workk");
        job.setJarByClass(SubmissionMR6Combine.class);
        //设置数据的输入和输出类型
        job.setInputFormatClass(CombineTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        //设置数据来源和计算结果的输出位置
        // 本地模拟二(本地环境+远程数据) 使用时运行Main行数即可
        CombineTextInputFormat.addInputPath(job,new Path("file:///e:\\data\\phone1.txt"));
         TextOutputFormat.setOutputPath(job,new Path("file:///e:\\result3"));
        //指定MAP和redurcer 任务的实现类//file:///e:\\result
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
