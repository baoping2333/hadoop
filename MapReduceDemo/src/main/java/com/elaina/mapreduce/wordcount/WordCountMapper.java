package com.elaina.mapreduce.wordcount;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @program: hadoop
 * @ClassName WordCountMapper
 * @description:
 * @author: wanbaoping
 * @create: 2021-08-22 15:04
 * @Version 1.0
 **/
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    Text k = new Text();
    IntWritable v = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        //1获取一行
        String line = value.toString();
        //2 切割
        String[] split = line.split(" ");
        //3 循环写出
        for (String word : split) {
            //疯转
            k.set(word);
            //写出
            context.write(k, v);
        }
    }
}
