package com.mary.mapr;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;


public class MapReduce {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        String[] interurbanCodes = {"351","342","345","349","343"};
        String[] internationalCodes = {"1038044","1037517","1099871","101212","104930"};

        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            String line = value.toString();
            String[] nums = new String[2];
            StringTokenizer tokenizer = new StringTokenizer(line, ",;\n");
            String callCode = "";

            int counter = 0;
            // false - caller
            while (tokenizer.hasMoreTokens()) {            // true - destination
                word.set(tokenizer.nextToken());
                switch (counter){
                    case 0:
                        nums[0] = word.toString();
                        break;
                    case 1:
                        nums[1] = word.toString();
                        break;
                    default:
                        break;
                }
                counter++;
                if (counter>3) break;
            }
            int i = 1;
            callCode = WhatKindOfCode(nums[0], nums[1]);
            word.set(callCode);
            output.collect(word, one);
        }

        private String WhatKindOfCode(String caller, String destination) {
            boolean callerStatus = false;        //true - international, false - interurban
            boolean destinationStatus = false;
            String callerCode = "";
            String destinationCode = "";

            for ( int m = 0; m < interurbanCodes.length; m++){
                if (destination.contains(interurbanCodes[m])){
                    destinationCode = interurbanCodes[m];
                }
            }
            for ( int n = 0; n < internationalCodes.length; n++){
                if (destination.contains(internationalCodes[n])){
                    destinationCode = internationalCodes[n];
                    destinationStatus = true;
                }
            }
            for ( int m = 0; m < interurbanCodes.length; m++){
                if (caller.contains(interurbanCodes[m])){
                    callerCode = interurbanCodes[m];
                }
            }
            for ( int n = 0; n < internationalCodes.length; n++){
                if (caller.contains(internationalCodes[n])){
                    callerCode = internationalCodes[n];
                    callerStatus = true;
                }
            }

            if (callerCode.equals(destinationCode)){
                return "same zone";
            } else if (destinationStatus){
                return "international call";
            } else {
                return "interurban call";
            }
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            int sum = 0;
            while (values.hasNext()) {
                sum += values.next().get();
            }
            output.collect(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {

        JobConf conf = new JobConf(MapReduce.class);
        conf.setJobName("wordcount");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(Map.class);
        conf.setCombinerClass(Reduce.class);
        conf.setReducerClass(Reduce.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }
}