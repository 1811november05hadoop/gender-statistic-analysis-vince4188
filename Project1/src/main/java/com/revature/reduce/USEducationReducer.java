package com.revature.reduce;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class USEducationReducer  extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
	
	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {
		
		double total = 0.0;

		double counter = 0.0;
		for(DoubleWritable value: values){
			//context.write(key, value);
			total += value.get();
			counter++;
		}
		
		context.write(key, new DoubleWritable(total/counter));
	}
}
