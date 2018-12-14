package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Question5Mapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		line = line.substring(1).substring(0, line.length()-3);
		String[] splits = line.split("\",\"");
		//same as before
		int year = 1960 + splits.length - 5;
		String code = "d";
		//code is used to determine which of the three lines it is
		if (line.toLowerCase().contains("educational attainment, no schooling, population 25+ years, female (%)")){
			code = "a";
		}
		else if(line.toLowerCase().contains("married women are required by law to obey their husbands (1=yes; 0=no)")){
			code = "b";
		}
		//had to use code cause all the lines were showing up from the lines following the one needed
		else if(line.toLowerCase().contains("sg.vaw.reas.zs")){
			code = "c";
		}
		//only want to write lines of the three codes
		if (code.equals("a") || code.equals("b") || code.equals("c")){
			if(splits.length > 4){
				context.write(new Text(splits[0]), new Text(code + year + "" + splits[splits.length-1]));
			}
			else{
				context.write(new Text(splits[0]), new Text(code + year + "NO VALID DATA"));
			}
		}
	}
}
