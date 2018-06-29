package com.he.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * @author heyc
 * @date 2018/6/8 16:33
 */
public class WordCount {

    /**
     * map
     */
    public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private Text outputKey = new Text();
        private IntWritable outputValue = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreElements()) {
                outputKey.set(itr.nextToken());
                context.write(outputKey, outputValue);
            }
        }
    }

    /**
     * reduce
     */
    public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable output = new IntWritable();
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            output.set(sum);
            context.write(key, output);
        }
    }

    /**
     * run
     */
    public boolean run(String args[]) throws Exception {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, getClass().getSimpleName());
        job.setJarByClass(getClass());

        // input
        Path inpath = new Path(args[0]);
        FileInputFormat.addInputPath(job, inpath);

        // map
        job.setMapperClass(WordCountMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //reduce
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //output
        Path outpath = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outpath);

        // submit
        return job.waitForCompletion(true);

    }

    public static void main(String[] args) throws Exception {
        boolean status = new WordCount().run(args);
        System.exit(status ? 0 : 1);
    }

}
