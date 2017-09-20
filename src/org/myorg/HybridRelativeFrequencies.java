
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


public class HybridRelativeFrequencies {

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
	        			//context.write(new Text(word+",*"), one);
	        		}
	        	}
	        	
	        }
	   }
	}
	       
	        
	 public static class Reduce extends Reducer<Text, IntWritable, Text, Text> {
		 
		 HashMap<String, Integer> finalMap = new HashMap<String, Integer>();
		 Integer sum = 0;
		 String  finalKey = "";
		 
	    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
	      throws IOException, InterruptedException {
	    	String[] pair = key.toString().split(",");
			String tmp = pair[0];
			
			if (!tmp.equals(finalKey)) {
				String str="";
				for (String k : finalMap.keySet()) {
					str += "(" + k + " " + finalMap.get(k).toString() + "/"
							+ sum.toString() + ")";
				}
				context.write(new Text(finalKey), new Text(str));
			
				sum = 0;
				finalMap.clear();
			}
			
			Integer count =0;
			for (IntWritable value : values) {
				count += value.get();
			}
			finalMap.put(pair[1],count);
			sum+=count;
			finalKey = tmp;
	        
	 }
	    protected void cleanup(Context context) throws IOException,
		InterruptedException {
			String output = "";
			for (String k : finalMap.keySet()) {
				output += "(" + k + " " + finalMap.get(k).toString() + "/"
						+ sum.toString() + ")";
			}
			context.write(new Text(finalKey), new Text(output));
		}
	 }
	        
	 public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	        
	        Job job = new Job(conf, "wordcount");
	        job.setJarByClass(HybridRelativeFrequencies.class);
	    
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
