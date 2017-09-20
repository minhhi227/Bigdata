
package org.myorg;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class AverageComputation {

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
	    private final static IntWritable one = new IntWritable(1);
	    private Text word = new Text();
	    HashMap<String, Integer> temp = new HashMap<String,Integer>();
	    
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        String line = value.toString();
	        String[] words = line.toString().split(" ");
	        if(!words[words.length-1].equals("-")){
	        	int num = Integer.parseInt(words[words.length-1]);
		        word.set(words[0]);
		        context.write(word, new IntWritable(num));
	        }
	   }
	}
	       
	        
	 public static class Reduce extends Reducer<Text, IntWritable, Text, DoubleWritable> {

	    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
	      throws IOException, InterruptedException {
	        int sum = 0;
	        int count =0;
	        for (IntWritable val : values) {
	            sum += val.get();
	            count++;
	        }
	        context.write(key, new DoubleWritable(sum/count));
	    }
	 }
	        
	 public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	        
	        Job job = new Job(conf, "wordcount");
	        job.setJarByClass(AverageComputation.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	        
	    job.setMapperClass(Map.class);
	    job.setReducerClass(Reduce.class);
	        
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	        
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	        
	    job.waitForCompletion(true);
	 }
}
