
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




public class StripeRelativeFrequencies {

	public static class Map extends Mapper<LongWritable, Text, Text, MapWritable> {
	    private final static IntWritable one = new IntWritable(1);
	    private Text word = new Text();
	    
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        String line = value.toString();
	        String[] words = line.toString().split(" ");
	        
	        for(int i=0 ; i< words.length - 1; i++){
	        	String word = words[i];
	        	HashMap<String, Integer> stripe = new HashMap<String, Integer>();
	        	
	        	for(int j = i+1; j<words.length ; j++){
	        		String temp = words[j];
	        		if(word.equals(temp)) break;
	        		else{
	        			if(!stripe.containsKey(temp)){
	        				stripe.put(temp, 1);
	        			}
	        			else{
	        				stripe.put(temp,stripe.get(temp)+1);
	        			}
	        		}
	        	}
	        	if (!stripe.isEmpty()) {
	        		MapWritable map = new MapWritable();
	        		for (Entry<String, Integer> entry : stripe.entrySet()) {
	        			if(entry.getKey() != null){
	        				map.put(new Text(entry.getKey()),new IntWritable(entry.getValue()));
	        			}
	        		}
	        		context.write(new Text(word), map);
	        		
	        	}
	        	
	        }
	   }
	}
	       
	        
	 public static class Reduce extends Reducer<Text, MapWritable, Text, Text> {
	    public void reduce(Text key, Iterable<MapWritable> values, Context context) 
	      throws IOException, InterruptedException {
	    	
	    	MapWritable finalMap = new MapWritable();
	    	Integer total = 0;
	    	for(MapWritable m : values){
	    		for(Writable w: m.keySet()){
	    			IntWritable count = (IntWritable)m.get(w);
	    			total += count.get();
	    			if(finalMap.containsKey(w)){
	    				count.set(((IntWritable)finalMap.get(w)).get()+count.get());
	    			}
	    			finalMap.put(w, count);
	    		}
	    	}
	    	String output = "";
	    	for(Writable k: finalMap.keySet()){
	    		IntWritable up = (IntWritable)finalMap.get(k);
	    		output +="(" + k.toString() + " " + String.valueOf(up.get()) + "/" + total.toString() + ") ";
	    	}
	    	 context.write(key, new Text(output));
	    	
	    }
	 }
	        
	 public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	        
	        Job job = new Job(conf, "wordcount");
	        job.setJarByClass(StripeRelativeFrequencies.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(MapWritable.class);
	        
	    job.setMapperClass(Map.class);
	    job.setReducerClass(Reduce.class);
	        
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	        
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	        
	    job.waitForCompletion(true);
	 }
}
