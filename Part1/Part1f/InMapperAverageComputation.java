
package org.myorg;

import java.io.DataInput;
import java.io.DataOutput;
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

public class InMapperAverageComputation {

	public static class Pair implements Writable { 
		private IntWritable key;
		private IntWritable value;
		
		public Pair(){
			super();
			key = new IntWritable();
			value = new IntWritable();
		}
		
		public Pair(IntWritable k, IntWritable v){
			key = k;
			value = v;
		}
		
		public IntWritable getKey() {
			return key;
		}

		public IntWritable getValue() {
			return value;
		}

	    @Override
	    public void write(DataOutput out) throws IOException {
	        key.write(out);
	        value.write(out);
	    }

		@Override
		public void readFields(DataInput input) throws IOException {
			 key.readFields(input);
		     value.readFields(input);
	    }
	}   
	public static class Map extends Mapper<LongWritable, Text, Text, Pair> {
	    private final static IntWritable one = new IntWritable(1);
	    private static MapWritable temp;
	    
	    public void setup(Context context){
	    	temp = new MapWritable();
	    }
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        String line = value.toString();
	        String[] words = line.split(" ");
	        String ip = words[0];
	        String number = words[words.length-1];
	        
	        if(!number.equals("-")){
	        	Text word = new Text(ip);
	        	if(!temp.containsKey(word)){
	        		temp.put(word,new Pair(new IntWritable(Integer.parseInt(number)), one) );
	        	}
	        	else{
	        		Pair pair = (Pair) temp.get(word);
	        		
	                int sum = pair.getKey().get() + Integer.parseInt(number);
	                int count = pair.getValue().get() + 1;
	                
	        		temp.put(word, new Pair(new IntWritable(sum), new IntWritable(count)));
	        	}
	        }
	        
	    }
	    public void cleanup(Context context) throws IOException, InterruptedException{
	    	for(Entry<Writable, Writable> i : temp.entrySet()){
	          	context.write((Text) i.getKey(), (Pair)i.getValue());
	        }
	    }
	 } 
	        
	 public static class Reduce extends Reducer<Text, Pair, Text, DoubleWritable> {

	    public void reduce(Text key, Iterable<Pair> values, Context context) 
	      throws IOException, InterruptedException {
	    	int sum = 0;
	        int count =0;
	        for (Pair val : values) {
	            sum += val.getKey().get();
	            count +=val.getValue().get();
	        }
	        context.write(key, new DoubleWritable(sum/count));
	    }
	 }
	        
	 public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	        
	        Job job = new Job(conf, "wordcount");
	        job.setJarByClass(InMapperAverageComputation.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Pair.class);
	        
	    job.setMapperClass(Map.class);
	    job.setReducerClass(Reduce.class);
	        
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	        
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	        
	    job.waitForCompletion(true);
	 }
}
