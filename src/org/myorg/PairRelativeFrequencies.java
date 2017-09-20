
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


public class PairRelativeFrequencies {

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
	    private final static IntWritable one = new IntWritable(1);
	    private Text word = new Text();
	    
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        String line = value.toString();
	        String[] words = line.toString().split(" ");
	        
	        for(int i=0 ; i< words.length - 1; i++){
	        	String word = words[i];
	        	for(int j = i+1; j<words.length ; j++){
	        		String temp = words[j];
	        		if(word.equals(temp)) break;
	        		else{
	        			context.write(new Text(word+","+temp), one);
	        			context.write(new Text(word+",*"), one);
	        		}
	        	}
	        	
	        }
	   }
	}
	       
	        
	 public static class Reduce extends Reducer<Text, IntWritable, Text, Text> {
		Integer sum = 0;
		 
	    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
	      throws IOException, InterruptedException {
	    	String[] pair = key.toString().split(",");
			String tmp = pair[1];
	        Integer count =0;
			if(tmp.equals("*")){
				sum =0;
				for (IntWritable val : values) {
					sum += val.get();
				}
			}
			else{
				for (IntWritable val : values) {
					count += val.get();
				}
			}
			
			if(tmp.equals("*")){
				context.write(key,new Text(sum.toString()));
			}
			else{
	        context.write(key, new Text(count.toString()+"/"+sum.toString()));
			}
	    }
	 }
	        
	 public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	        
	        Job job = new Job(conf, "wordcount");
	        job.setJarByClass(PairRelativeFrequencies.class);
	    
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
