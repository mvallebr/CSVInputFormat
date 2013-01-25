package org.apache.hadoop.mapreduce.lib.input.test;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class ImporterMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
	private static final Logger logger = Logger.getLogger(ImporterMapper.class
			.getName());

	public void map(LongWritable key, Text value, Context context) throws IOException,
			InterruptedException {
		logger.info("ImporterMapper");
		logger.info("key=" + key);
		logger.info("value=" + value);
		context.write(new LongWritable(1l), new Text("1"));

	}
}