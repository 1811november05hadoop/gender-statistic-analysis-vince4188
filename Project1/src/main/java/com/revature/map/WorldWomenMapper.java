package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WorldWomenMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		line = line.substring(1);
		line = line.substring(0, line.length()-2);
		
		if(line.contains("completed upper secondary") && line.contains("female") && !line.contains("cumulative")){
			//System.out.println(line);
			String[] splits = line.split("\",\"");
			String country = splits[0];
			double percent = 0.0;
			int year = 1960 + splits.length - 4;
			//arrays of length 4 have no data
			if(splits.length > 4){
				//the last section in the array should contain a value, but just in case...
				for (int i = splits.length-1; i > splits.length - 4; i--){
					if (!splits[i].trim().equals("")){
						percent = Double.parseDouble(splits[i]);
						break;
					}
				}
				//if (percent < 30.0){
				context.write(new Text(country + "(" + year + "): "), new Text(Double.toString(percent) + "%"));
			}
			else{
				context.write(new Text(country + "(" + year + "): "), new Text("NO VALID DATA"));
			}		
		}
	}

}
