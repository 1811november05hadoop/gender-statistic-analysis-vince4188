package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class EmploymentFemaleMapper extends Mapper<LongWritable, Text, Text, DoubleWritable>{

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		line = line.substring(1).substring(0, line.length()-3);
		String[] splits = line.split("\",\"");
		
		if (line.toLowerCase().contains("unemployment") && line.toLowerCase().contains(" female ") && line.toLowerCase().contains("ilo")){
			String newKey = "(" + splits[0] + ") Percent change in unemployment,";
			if(line.contains("youth")){
				newKey += " youth female(% female labor force age 15-24)";
			}
			else{
				newKey += " female(% female labor force)";
			}
			newKey += "(ILO)(2000-2016): ";
			
			if (splits.length > 4){
				int count = 0;
				double prevVal = 0.0;
				for (int i = 44; i < splits.length; i++){
					if(!splits[i].isEmpty() && !splits[i].trim().trim().equals("\"")) {
						count++;
						if(prevVal != 0.0){
							context.write(new Text(newKey), new DoubleWritable(Double.parseDouble(splits[i]) - prevVal));
						}
						prevVal = Double.parseDouble(splits[i]);
					}
				}
				//if count is 0 means there was no data after year 2000
				if (count == 0){
					context.write(new Text(newKey), new DoubleWritable(-1.0));
				}
			}
			else{
				context.write(new Text(newKey), new DoubleWritable(-1.0));
			}
		}
	}
}

