package org.apache.hadoop.mapreduce.lib.input.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CSVLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CSVNLineInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.MultithreadedMapper;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class CSVTestRunner extends Configured implements Tool {
	private static final Logger logger = Logger.getLogger(CSVTestRunner.class.getName());

	private static final String INPUT_PATH_PREFIX = "./src/test/resources/";
	//private static final String INPUT_PATH_PREFIX = "/tmp/importer_tests/";

	public static void main(String[] args) throws Exception {
		int res = -1;
		try {
			logger.info("Initializing CSV Test Runner");
			CSVTestRunner importer = new CSVTestRunner();

			// Let ToolRunner handle generic command-line options and run hadoop
			res = ToolRunner.run(new Configuration(), importer, args);
			logger.info("ToolRunner finished running hadoop");

		} catch (Throwable e) {
			e.printStackTrace();
		} finally {
			logger.info("Quitting with error code " + res);
			System.exit(res);
		}
	}

	public int run(String[] args) throws Exception {

		getConf().set(CSVLineRecordReader.FORMAT_DELIMITER, "\"");
		getConf().set(CSVLineRecordReader.FORMAT_SEPARATOR, ",");
		getConf().setInt(CSVNLineInputFormat.LINES_PER_MAP, 40000);
		getConf().setBoolean(CSVLineRecordReader.IS_ZIPFILE, false);
		Job csvJob = new Job(getConf(), "csv_test_job");
		csvJob.setJarByClass(CSVTestRunner.class);
		csvJob.setNumReduceTasks(0);		
		MultithreadedMapper.setMapperClass(csvJob, TestMapper.class);
		MultithreadedMapper.setNumberOfThreads(csvJob, 1);
		csvJob.setMapperClass(MultithreadedMapper.class);
		// To run without multithread, use the following line instead of the 3
		// above
		// csvJob.setMapperClass(TestMapper.class);		
		csvJob.setInputFormatClass(CSVNLineInputFormat.class);
		csvJob.setOutputFormatClass(NullOutputFormat.class);
		FileInputFormat.setInputPaths(csvJob, new Path(INPUT_PATH_PREFIX));
		logger.info("Process will begin");
		
		csvJob.waitForCompletion(true);

		logger.info("Process ended");

		return 0;
	}

	public CSVTestRunner() {
		this(null);
	}

	public CSVTestRunner(Configuration conf) {
		super(conf);
	}

}