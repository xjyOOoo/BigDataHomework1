package com.example.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MapReduceHomework {

    public static void main(String[] args) throws Exception {
        String inputPath = args[0];
        String tempOutputPath = args[1];
        String finalOutputPath = args[2];
        String stopWordsPath = args[3];
        String task = args[4];
        Configuration conf = new Configuration();

        if ("task1".equalsIgnoreCase(task)) {
            Job job1 = Job.getInstance(conf, "Stock Count");
            job1.setJarByClass(MapReduceHomework.class);
            job1.setMapperClass(StockCountMapper.class);
            job1.setCombinerClass(StockCountReducer.class); // add Combiner
            job1.setReducerClass(StockCountReducer.class);
            job1.setOutputKeyClass(Text.class);
            job1.setOutputValueClass(LongWritable.class);
            FileInputFormat.addInputPath(job1, new Path(inputPath));
            FileOutputFormat.setOutputPath(job1, new Path(tempOutputPath));
            boolean success = job1.waitForCompletion(true);
            if (!success) {
                System.exit(1);
            }

            Job job2 = Job.getInstance(conf, "Sort Stocks by Count");
            job2.setJarByClass(MapReduceHomework.class);
            job2.setMapperClass(SortByCountMapper.class);
            job2.setReducerClass(SortByCountReducer.class);
            job2.setSortComparatorClass(LongWritable.DecreasingComparator.class);
            job2.setOutputKeyClass(LongWritable.class);
            job2.setOutputValueClass(Text.class);
            job2.setNumReduceTasks(1);
            FileInputFormat.addInputPath(job2, new Path(tempOutputPath));
            FileOutputFormat.setOutputPath(job2, new Path(finalOutputPath));
            success = job2.waitForCompletion(true);
            if (!success) {
                System.exit(1);
            }

        } else if ("task2".equalsIgnoreCase(task)) {
            conf.set("stopwords.path", stopWordsPath);
            Job job1 = Job.getInstance(conf, "Word Count");
            job1.setJarByClass(MapReduceHomework.class);
            job1.setMapperClass(WordCountMapper.class);
            job1.setCombinerClass(WordCountReducer.class);
            job1.setReducerClass(WordCountReducer.class);
            job1.setOutputKeyClass(Text.class);
            job1.setOutputValueClass(LongWritable.class);
            FileInputFormat.addInputPath(job1, new Path(inputPath));
            FileOutputFormat.setOutputPath(job1, new Path(tempOutputPath));
            boolean success = job1.waitForCompletion(true);
            if (!success) {
                System.exit(1);
            }

            Job job2 = Job.getInstance(conf, "Sort Words by Count");
            job2.setJarByClass(MapReduceHomework.class);
            job2.setMapperClass(SortByCountMapper.class);
            job2.setReducerClass(SortByCountReducerLimited.class);
            job2.setSortComparatorClass(LongWritable.DecreasingComparator.class);
            job2.setOutputKeyClass(LongWritable.class);
            job2.setOutputValueClass(Text.class);
            job2.setNumReduceTasks(1);
            FileInputFormat.addInputPath(job2, new Path(tempOutputPath));
            FileOutputFormat.setOutputPath(job2, new Path(finalOutputPath));
            success = job2.waitForCompletion(true);
            if (!success) {
                System.exit(1);
            }
        } else {
            System.err.println("Invalid task specified. Please input task1 or task2.");
            System.exit(-1);
        }
    }
}