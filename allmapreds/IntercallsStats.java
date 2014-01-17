package com.mary.mapr;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class MapReduce {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private int counter = 0;
        String[] interurbanCodes = {"351","342","345","349"};
        String[] internationalCodes = {"1038044","1037517","1099871","101212","104930"};

        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            String line = value.toString();
            String[] nums = new String[2];
            StringTokenizer tokenizer = new StringTokenizer(line, ";\n");
            String callType = "";
            while (tokenizer.hasMoreTokens()) {
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
            }
            callType = WhatKindOfCall(nums[0], nums[1]);
            word.set(callType);
            output.collect(word, one);
        }

        private String WhatKindOfCall(String caller, String destination) {
            boolean callerStatus = false;        //true - international, false - interurban
            boolean destinationStatus = false;
            String callerCode = "";
            String destinationCode = "";
            for ( int i = 0; i < interurbanCodes.length; i++){
                if (caller.contains(interurbanCodes[i])){
                    callerStatus = false;
                    callerCode = interurbanCodes[i];
                }
                if (destination.contains(interurbanCodes[i])){
                    destinationStatus = false;
                    destinationCode = interurbanCodes[i];
                }
            }
            for ( int j = 0; j < internationalCodes.length; j++){
                if (caller.contains(internationalCodes[j])){
                    callerStatus = true;
                    callerCode = internationalCodes[j];
                }
                if (destination.contains(internationalCodes[j])){
                    destinationStatus = true;
                    destinationCode = internationalCodes[j];
                }
            }
            if ( callerStatus == destinationStatus ){
                if ( callerCode.equals(destinationCode) ){
                    return "samecode";
                }
                else return "interurban";
            }
            else return "international";
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


