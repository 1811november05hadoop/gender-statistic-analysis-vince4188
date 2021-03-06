package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class USEducationMapper  extends Mapper<LongWritable, Text, Text, DoubleWritable>{

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		
		if (line.toLowerCase().contains("united states")){
			if(line.toLowerCase().contains("educational attainment") && line.contains("female") && !line.contains("cumulative")){
				line = line.substring(1).substring(0, line.length()-3);
				String[] splits = line.split("\",\"");
				
				double prevVal = 0.0;
				//purpose of this for loop is to cycle through all the data values and get the difference and send it to the reducer to sum up all the values
				for (int i = 44; i < splits.length; i++){
					if(!splits[i].isEmpty()) {
						//to check if first value, if so there can be no diference
						if(prevVal == 0.0){
							prevVal = Double.parseDouble(splits[i]);
						}
						else{
							context.write(new Text(splits[2] + ": "), new DoubleWritable(Double.parseDouble(splits[i]) - prevVal));
							prevVal = Double.parseDouble(splits[i]);
						}
					}
				}
			}
		}
	}
}
