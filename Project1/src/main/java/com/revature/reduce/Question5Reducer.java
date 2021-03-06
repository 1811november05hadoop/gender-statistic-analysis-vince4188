package com.revature.reduce;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Question5Reducer extends Reducer<Text, Text, Text, Text>{

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		String threeStr = "(" + key.toString() + ") ";
		String twoStr = threeStr.toString();
		String oneStr = twoStr.toString();
		String oneVal = "", twoVal = "", threeVal = "";
		int counter = 0;
		for (Text value: values){
			//splits the value text into different chunks to get the values used
			String code = value.toString().substring(0, 1);
			String year = value.toString().substring(1, 5);
			String val = value.toString().substring(5);
			if(code.equals("a")){
				oneStr += "Females with no schooling(population 25+)(" + year + "): ";
				if (!val.contains("NO VALID DATA")){
					oneVal = val + "%";
					counter++;
				}
			}
			else if(code.equals("b")){
				twoStr += "Married women are required by law to obey their husbands(" + year + "): ";
				if (val.equals("1")){
					twoVal = "YES";
					counter++;
				}
				else if(val.equals("0")){
					twoVal = "NO";
					counter++;
				}
			}
			else if(code.equals("c")){
				threeStr += "Women who believe a husband is justified in beating his wife (any of five reasons)(" + year + "): ";
				if (!val.contains("NO VALID DATA")){
					threeVal = val + "%" +
					"\n===========================================================";
					counter++;
				}
			}
		}
		//I only want to return data if all of the three are available
		if(counter == 3){
			context.write(new Text(oneStr), new Text(oneVal));
			context.write(new Text(twoStr), new Text(twoVal));
			context.write(new Text(threeStr), new Text(threeVal));
		}
		
	}
}
