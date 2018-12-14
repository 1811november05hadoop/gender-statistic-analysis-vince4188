package com.revature;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import com.revature.map.AlphabetPartitioner;
import com.revature.map.WordMapper;
import com.revature.reduce.MaxReducer;
import com.revature.reduce.SumReducer;

public class WordCountRunner extends Configured implements Tool{

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2){
			System.err.println("Usage: WordCount <input dir> <output dir>");
			return -1;
		}
		
		//The MapReduce job object
		Job job = new Job();
		
		//The class that contains the main method.
		job.setJarByClass(WordCount.class);
		
		job.setJobName("Welcome to the MapReduce");
		
		//Set input and output paths
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//Specify the mapper and reducer class
		job.setMapperClass(WordMapper.class);
		//job.setReducerClass(SumReducer.class);
		
		//Partitioner class
		job.setPartitionerClass(AlphabetPartitioner.class);
		//Amount of Reducers
		job.setNumReduceTasks(3);
		
		job.setReducerClass(MaxReducer.class);
		job.setCombinerClass(SumReducer.class);
		
		//Specify job final output key value types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//Run and check success
		boolean success = job.waitForCompletion(true);
		return success ? 0: 1;
	}

	
}
