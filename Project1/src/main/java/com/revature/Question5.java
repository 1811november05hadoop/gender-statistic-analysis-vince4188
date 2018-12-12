package com.revature;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.revature.map.Question5Mapper;
import com.revature.reduce.Question5Reducer;

public class Question5 {

	public static void main(String[] args) throws Exception{
		
		if (args.length != 2){
			System.err.println("Usage: Question5 <input dir> <output dir>");
			System.exit(-1);
		}
		
		//The MapReduce job object
		Job job = new Job();
		
		//The class that contains the main method.
		job.setJarByClass(USEducation.class);
		
		job.setJobName("Is beating your wife okay?");
		
		//Set input and output paths
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//Mapper and Reducer
		job.setMapperClass(Question5Mapper.class);
		
		job.setReducerClass(Question5Reducer.class);
		
		//Specify job final output key value types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);		
		
		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0: 1);
	}
}
