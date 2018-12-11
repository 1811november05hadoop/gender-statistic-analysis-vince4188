package com.revature;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.revature.map.EmploymentMaleMapper;
import com.revature.reduce.EmploymentMaleFemaleReducer;

public class EmploymentMale {

	public static void main(String[] args) throws Exception{
		
		if (args.length != 2){
			System.err.println("Usage: EmploymentMale <input dir> <output dir>");
			System.exit(-1);
		}
		
		//The MapReduce job object
		Job job = new Job();
		
		//The class that contains the main method.
		job.setJarByClass(EmploymentMale.class);
		
		job.setJobName("Employment Males Worldwide");
		
		//Set input and output paths
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//Mapper and Reducer
		job.setMapperClass(EmploymentMaleMapper.class);
		
		job.setReducerClass(EmploymentMaleFemaleReducer.class);
		
		//Specify job final output key value types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);		
		
		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0: 1);
	}
}
