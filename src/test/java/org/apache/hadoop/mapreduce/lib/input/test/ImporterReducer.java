package org.apache.hadoop.mapreduce.lib.input.test;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;
;

public class ImporterReducer extends
		Reducer<LongWritable, Text, NullWritable, NullWritable> {
	private static final Logger logger = Logger.getLogger(ImporterReducer.class.getName());


	public void reduce(LongWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		logger.info("LineCountReducer");
				
		
	}
}