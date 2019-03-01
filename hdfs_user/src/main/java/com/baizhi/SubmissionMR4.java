package com.baizhi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class SubmissionMR4 {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        //创建MR 任务对象
        Configuration entries = new Configuration();
        entries.set("fs.defaultFS", "hdfs://hadoop:9000/");
        entries.set("mapreduce.job.jar", "E:\\nameSpaceTwo\\hdfs_user\\target\\hdfs_user-1.0-SNAPSHOT.jar");
        entries.set("mapreduce.framework.name", "yarn");
        entries.set("yarn.resourcemanager.hostname", "hadoop");
        entries.set("yarn.nodemanager.aux-services", "mapreduce_shuffle");
        entries.set("mapreduce.app-submission.cross-platform", "true"); //开启了跨平台支持
        entries.set("dfs.replication", "1");

        Job job = Job.getInstance(entries,"MyFile");
        job.setJarByClass(SubmissionMR4.class);
        //设置数据的输入和输出类型
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        //设置数据来源和计算结果的输出位置
        // 本地模拟二(本地环境+远程数据) 使用时运行Main行数即可
        TextInputFormat.addInputPath(job,new Path("hdfs://hadoop:9000/baizhi/phone.log"));
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop:9000"),entries);
        Path path = new Path("hdfs://hadoop:9000/baizhi/result");
        boolean exists = fileSystem.exists(path);
        if(exists){
            fileSystem.delete(path,true);
        }
        TextOutputFormat.setOutputPath(job,path);
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
