package com.bazhi;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MyMapper extends Mapper<LongWritable, Text,Text,Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        final String regex = "^(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3})\\s-\\s-\\s\\[(.*)\\s.*]\\s\"[A-Z]*\\s(.*)\\sHTTP\\/1.1\"\\s(\\d{3}).*$";
        final String string = value.toString();

        final Pattern pattern = Pattern.compile(regex, Pattern.MULTILINE);
        final Matcher matcher = pattern.matcher(string);

        while (matcher.find()) {
            String valueOut = matcher.group(2) + "  " + matcher.group(3) + " " + matcher.group(4);
            context.write(new Text(matcher.group(1)), new Text(valueOut));

        }
    }
}
