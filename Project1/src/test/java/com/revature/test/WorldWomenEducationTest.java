package com.revature.test;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import com.revature.map.WorldWomenMapper;

public class WorldWomenEducationTest {
	
	private MapDriver<LongWritable, Text, Text, Text> mapDriver;
	
	@Before
	public void setUp() {
		WorldWomenMapper mapper = new WorldWomenMapper();
		mapDriver = new MapDriver<>();
		mapDriver.setMapper(mapper);	
	}
	
	@Test
	public void testMapper1() {
		//Mock Input
		mapDriver.withInput(new LongWritable(2), new Text("\"Zimbabwe\",\"ZWE\",\"Educational attainment, completed upper secondary, population 25+ years, female (%)\",\"SE.SEC.HIAT.UP.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"0.56946\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"1.35334\",\"\",\"1.47717\",\"\",\"\","));				
		//Expected Output
		mapDriver.withOutput(new Text("(Zimbabwe) completed upper secondary  (population female 25+)(2014):"), new Text("1.47717%"));
		
		mapDriver.runTest();
	}
	
	@Test
	public void TestMapper2() {
		//Mock Input
		mapDriver.withInput(new LongWritable(1), new Text("\"Singapore\",\"SGP\",\"Educational attainment, completed Bachelor's or equivalent, population 25+ years, female (%)\",\"SE.TER.HIAT.BA.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"2\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"3.49191\",\"\",\"\",\"\",\"\",\"5.77955\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"25.77032\",\"17.08729\",\"17.82677\",\"19.48114\",\"20.30797\",\"21.60116\",\"22.72257\",\"23.46401\",\"25.15091\",\"25.47448\",\"26.13423\",\"\","));		
		//Expected Output
		mapDriver.withOutput(new Text("(Singapore) completed Bachelor's  (population female 25+)(2015):"), new Text("26.13423%"));
		
		mapDriver.runTest();
	}
	
	@Test
	public void TestMapper3() {
		//Mock Input
		mapDriver.withInput(new LongWritable(1), new Text("\"Bolivia\",\"BOL\",\"Educational attainment, completed Master's or equivalent, population 25+ years, female (%)\",\"SE.TER.HIAT.MS.FE.ZS\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"1.21635\",\"\",\"\",\"\",\"\","));
		//Expected Output
		mapDriver.withOutput(new Text("(Bolivia) completed Master's  (population female 25+)(2012):"), new Text("1.21635%"));
		
		mapDriver.runTest();
	}

}
