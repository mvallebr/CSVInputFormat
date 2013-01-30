package org.apache.hadoop.mapreduce.lib.input;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Configurable CSV line reader. Variant of TextInputReader that reads CSV
 * lines, even if the CSV has multiple lines inside a single column
 * 
 *
 * @author mvallebr
 *
 */
public class CSVTextInputFormat extends FileInputFormat<LongWritable, List<Text>> {

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.InputFormat#createRecordReader(org.apache.hadoop.mapreduce.InputSplit, org.apache.hadoop.mapreduce.TaskAttemptContext)
	 */
	@Override
	public RecordReader<LongWritable, List<Text>> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException {
		Configuration conf = context.getConfiguration();
		String quote = conf.get(CSVLineRecordReader.FORMAT_DELIMITER);
		String separator = conf.get(CSVLineRecordReader.FORMAT_SEPARATOR);
		if (null == quote || null == separator) {
			throw new IOException("CSVTextInputFormat: missing parameter delimiter/separator");
		}
		return new CSVLineRecordReader();
	}

}