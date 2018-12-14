package com.revature.reduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	
	public static volatile String CURRENT_MAX_WORD = null;
	public static volatile int CURRENT_MAX_COUNT = Integer.MIN_VALUE;
	
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		for (IntWritable value: values) {
			if(value.get() > CURRENT_MAX_COUNT) {
				CURRENT_MAX_WORD = key.toString();
				CURRENT_MAX_COUNT = value.get();
			}
			/*
			CURRENT_MAX_COUNT = (value.get() > CURRENT_MAX_COUNT) ?
						value.get(): CURRENT_MAX_COUNT;
			CURRENT_MAX_WORD = (value.get() > CURRENT_MAX_COUNT) ?
					key.toString(): CURRENT_MAX_WORD;
			*/
		}
		
		context.write(new Text(CURRENT_MAX_WORD), new IntWritable(CURRENT_MAX_COUNT));
	}
}
