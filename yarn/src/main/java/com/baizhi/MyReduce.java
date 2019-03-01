package com.baizhi;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MyReduce extends Reducer<Text, IntWritable,Text,IntWritable> {
    /**
     * 计算方法
     *
     * @param key  单词
     * @param values  key相同的 value自动合并到一个集合中 welcome [1,1]
     * @param context  上下文对象
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum=0;
        for (IntWritable value : values) {
            sum+=value.get();
        }
        context.write(key,new IntWritable(sum));
    }
}
