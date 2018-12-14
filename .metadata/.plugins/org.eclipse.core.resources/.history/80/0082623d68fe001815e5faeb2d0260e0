package com.revature.map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class AlphabetPartitioner extends Partitioner<Text, IntWritable>{

	private static char FIRST_SECTION_END = 'g';
	
	private static char SECOND_SECTION_START = 'h';
	private static char SECOND_SECTION_END = 'p';
	
	
	@Override
	public int getPartition(Text key, IntWritable value, int numReduceTasks) {
		char firstCharacter = key.toString().charAt(0);
		
		if(firstCharacter <= FIRST_SECTION_END){
			//send to first reducer
			return 0;
		}
		else if(firstCharacter >= SECOND_SECTION_START && 
				firstCharacter <= SECOND_SECTION_END){
			return 1;
		}
		else{
			return 2;
		}
	}
	
}
