package com.revature;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;



public class WordCount {
	
	public static void main(String[] args) throws Exception{
		
		int exitCode = ToolRunner.run(new Configuration(), new WordCountRunner(), args);
		
		System.exit(exitCode);
	}

}
