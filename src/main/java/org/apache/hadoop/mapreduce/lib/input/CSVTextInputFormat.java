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
 */
public class CSVTextInputFormat extends FileInputFormat<LongWritable, List<Text>> {

	public static final String FORMAT_DELIMITER = "mapreduce.csvinput.delimiter";
	public static final String FORMAT_SEPARATOR = "mapreduce.csvinput.separator";
	public static final String IS_ZIPFILE = "mapreduce.csvinput.zipfile";

	@Override
	public RecordReader<LongWritable, List<Text>> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException {
		Configuration conf = context.getConfiguration();
		String quote = conf.get(FORMAT_DELIMITER);
		String separator = conf.get(FORMAT_SEPARATOR);		
		if (null == quote || null == separator) {
			throw new IOException(
					"CSVTextInputFormat: missing parameter delimiter/separator");
		}
		return new CSVLineRecordReader();
	}

//	@Override
//	protected boolean isSplitable(JobContext context, Path file) {
//		CompressionCodec codec = new CompressionCodecFactory(
//				context.getConfiguration()).getCodec(file);
//		return codec == null;
//	}

}