package org.apache.hadoop.mapreduce.lib.input.test;

import org.junit.Before;
import org.junit.Test;


public class BasicTest {
	@Before
	public void setUp() throws Exception {

	}

	@Test
	public void testFullProcess() throws Exception {
		String[] args = {};// {"runSolrOnly"};
		// Let ToolRunner handle generic command-line options
		CSVTestRunner.main(args);
	}

}
