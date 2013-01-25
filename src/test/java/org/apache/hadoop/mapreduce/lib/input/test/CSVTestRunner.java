package org.apache.hadoop.mapreduce.lib.input.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CSVTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class CSVTestRunner extends Configured implements Tool {
	private static final Logger logger = Logger.getLogger(CSVTestRunner.class
			.getName());

	private static final String INPUT_PATH_PREFIX = "./src/test/resources/";

	public static void main(String[] args) throws Exception {
		int res = -1;
		try {
			logger.info("Initializing Importer WTF");
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

		getConf().set("mapreduce.csvinput.quote", "\"");
		getConf().set("mapreduce.csvinput.separator", ",");
		Job importerJob = new Job(getConf(), "dmp_normalizer");
		importerJob.setJarByClass(CSVTestRunner.class);
		importerJob.setMapperClass(TestMapper.class);

		importerJob.setInputFormatClass(CSVTextInputFormat.class);
		importerJob.setOutputFormatClass(NullOutputFormat.class);
		FileInputFormat.setInputPaths(importerJob, new Path(INPUT_PATH_PREFIX));

		ControlledJob importerControlledJob = new ControlledJob(importerJob,
				null);
		JobControl jc = new JobControl("DMPImporter");
		jc.addJob(importerControlledJob);

		logger.info("Starting controller thread ");
		// start the controller in a different thread
		Thread theController = new Thread(jc);
		theController.start();
		logger.info("Controller thread started ");
		// poll until everything is done,
		// in the meantime justs output some status message
		int c = 0;
		final int SECOND = 1000; // number of miliseconds in a second
		final int NUM_SECONDs = 10; // number of seconds to wait before printing
									// status
		while (!jc.allFinished()) {
			// Sleep sometime before printing status again
			try {
				Thread.sleep(SECOND);
			} catch (Exception e) {
			}
			if ((c++ % NUM_SECONDs != 0) || (jc.allFinished()))
				continue;
			logger.info("-------------------------------------------------------");
			logger.info("Jobs in waiting state: "
					+ jc.getWaitingJobList().size());
			logger.info("Jobs in ready state: " + jc.getReadyJobsList().size());
			logger.info("Jobs in running state: "
					+ jc.getRunningJobList().size());
			logger.info("Jobs in success state: "
					+ jc.getSuccessfulJobList().size());
			logger.info("Jobs in failed state: " + jc.getFailedJobList().size());
			logger.info("-------------------------------------------------------");
		}

		if (importerControlledJob.getJobState() != ControlledJob.State.FAILED
				&& importerControlledJob.getJobState() != ControlledJob.State.DEPENDENT_FAILED
				&& importerControlledJob.getJobState() != ControlledJob.State.SUCCESS) {
			String states = "importerControlledJob:  "
					+ importerControlledJob.getJobState() + "\n";
			throw new Exception(
					"The state of importerControlledJob is not in a complete state\n"
							+ states);
		}

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