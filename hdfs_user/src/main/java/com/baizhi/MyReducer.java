package com.baizhi;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MyReducer extends Reducer<Text,MyFile,Text,MyFile> {
    @Override
    protected void reduce(Text key, Iterable<MyFile> values, Context context) throws IOException, InterruptedException {
       Long upload=0L;
       Long download=0L;
        for (MyFile value : values) {
            upload+=value.getUpload();
            download+=value.getDownload();
        }
        context.write(key,new MyFile(upload,download));
    }
}
