package org.apache.hadoop.mapreduce.lib.input.test;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class TestMapper extends Mapper<LongWritable, List<Text>, LongWritable, List<Text>> {
	private static final Logger logger = Logger.getLogger(TestMapper.class.getName());

	public void map(LongWritable key, List<Text> values, Context context) throws IOException, InterruptedException {
		logger.info("TestMapper");
		logger.info("key=" + key);
		int i = 0;
		for (Text val : values)
			logger.info("key=" + key + " val[" + (i++) + "] = " + val);
		// context.write(new LongWritable(1l), new Text("1"));

	}
}