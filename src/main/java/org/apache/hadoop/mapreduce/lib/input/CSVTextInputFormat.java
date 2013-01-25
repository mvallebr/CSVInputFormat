package org.apache.hadoop.mapreduce.lib.input;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Configurable CSV line reader. Variant of TextInputReader that reads CSV
 * lines, even if the CSV has multiple lines inside a single column
 * 
 */
public class CSVTextInputFormat extends FileInputFormat<LongWritable, Text> {

	private static final String FORMAT_QUOTE = "mapreduce.csvinput.quote";
	private static final String FORMAT_SEPARATOR = "mapreduce.csvinput.separator";

	@Override
	public RecordReader<LongWritable, Text> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException {
		Configuration conf = context.getConfiguration();
		String quote = conf.get(FORMAT_QUOTE);
		String separator = conf.get(FORMAT_SEPARATOR);
		if (null == quote || null == separator) {
			throw new IOException(
					"CSVTextInputFormat: missing parameter quote/separator");
		}
		return new CSVLineRecordReader();
	}

	@Override
	protected boolean isSplitable(JobContext context, Path file) {
		CompressionCodec codec = new CompressionCodecFactory(
				context.getConfiguration()).getCodec(file);
		return codec == null;
	}

}