package org.apache.hadoop.mapreduce.lib.input;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.zip.ZipInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Reads a CSV line. CSV files could be multiline, as they may have line breaks
 * inside a column
 */
public class CSVLineRecordReader extends RecordReader<LongWritable, List<Text>> {
	// private static final Log LOG =
	// LogFactory.getLog(CSVLineRecordReader.class);
	public static final String FORMAT_DELIMITER = "mapreduce.csvinput.delimiter";
	public static final String FORMAT_SEPARATOR = "mapreduce.csvinput.separator";
	public static final String IS_ZIPFILE = "mapreduce.csvinput.zipfile";
	public static final String DEFAULT_DELIMITER = "\"";
	public static final String DEFAULT_SEPARATOR = ",";
	public static final boolean DEFAULT_ZIP = true;

	private CompressionCodecFactory compressionCodecs = null;
	private long start;
	private long pos;
	private long end;
	protected Reader in;
	private LongWritable key = null;
	private List<Text> value = null;
	private String delimiter;
	private String separator;
	private Boolean isZipFile;
	private InputStream is;

	public CSVLineRecordReader() {

	}

	public CSVLineRecordReader(InputStream is, Configuration conf) throws IOException {
		init(is, conf);
	}

	public void init(InputStream is, Configuration conf) throws IOException {
		this.delimiter = conf.get(FORMAT_DELIMITER, DEFAULT_DELIMITER);
		this.separator = conf.get(FORMAT_SEPARATOR, DEFAULT_SEPARATOR);
		this.isZipFile = conf.getBoolean(IS_ZIPFILE, DEFAULT_ZIP);
		if (isZipFile) {
			@SuppressWarnings("resource")
			ZipInputStream zis = new ZipInputStream(new BufferedInputStream(is));
			zis.getNextEntry();
			is = zis;
		}
		this.is = is;
		this.in = new BufferedReader(new InputStreamReader(is));
	}

	protected int readLine(List<Text> values) throws IOException {
		values.clear();// Empty value columns list
		char c;
		int numRead = 0;
		boolean insideQuote = false;
		StringBuffer sb = new StringBuffer();
		int i;
		int quoteOffset = 0, delimiterOffset = 0;
		// Reads each char from input stream unless eof was reached
		while ((i = in.read()) != -1) {
			c = (char) i;
			numRead++;
			sb.append(c);
			// Check quotes, as delimiter inside quotes don't count
			if (c == delimiter.charAt(quoteOffset)) {
				quoteOffset++;
				if (quoteOffset >= delimiter.length()) {
					insideQuote = !insideQuote;
					quoteOffset = 0;
				}
			} else {
				quoteOffset = 0;
			}
			// Check delimiters, but only those outside of quotes
			if (!insideQuote) {
				if (c == separator.charAt(delimiterOffset)) {
					delimiterOffset++;
					if (delimiterOffset >= separator.length()) {
						foundDelimiter(sb, values, true);
						delimiterOffset = 0;
					}
				} else {
					delimiterOffset = 0;
				}
				// A new line outside of a quote is a real csv line breaker
				if (c == '\n') {
					break;
				}
			}
		}
		foundDelimiter(sb, values, false);
		return numRead;
	}

	protected void foundDelimiter(StringBuffer sb, List<Text> values, boolean takeDelimiterOut)
			throws UnsupportedEncodingException {
		// Found a real delimiter
		Text text = new Text();
		String val = (takeDelimiterOut) ? sb.substring(0, sb.length() - separator.length()) : sb.toString();
		if (val.startsWith(delimiter) && val.endsWith(delimiter)) {
			val = (val.length() - (2 * delimiter.length()) > 0) ? val.substring(delimiter.length(), val.length()
					- (2 * delimiter.length())) : "";
		}
		text.append(val.getBytes("UTF-8"), 0, val.length());
		values.add(text);
		// Empty string buffer
		sb.setLength(0);
	}

	public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
		FileSplit split = (FileSplit) genericSplit;
		Configuration job = context.getConfiguration();

		start = split.getStart();
		end = start + split.getLength();
		final Path file = split.getPath();
		compressionCodecs = new CompressionCodecFactory(job);
		final CompressionCodec codec = compressionCodecs.getCodec(file);

		// open the file and seek to the start of the split
		FileSystem fs = file.getFileSystem(job);
		FSDataInputStream fileIn = fs.open(split.getPath());

		if (codec != null) {
			is = codec.createInputStream(fileIn);
			end = Long.MAX_VALUE;
		} else {
			if (start != 0) {
				fileIn.seek(start);
			}
			is = fileIn;
		}

		this.pos = start;
		init(is, job);
	}

	public boolean nextKeyValue() throws IOException {
		if (key == null) {
			key = new LongWritable();
		}
		key.set(pos);

		if (value == null) {
			value = new ArrayListTextWritable();
		}
		while (true) {
			if (pos >= end)
				return false;
			int newSize = 0;
			newSize = readLine(value);
			pos += newSize;
			if (newSize == 0) {
				if (isZipFile) {
					ZipInputStream zis = (ZipInputStream) is;
					if (zis.getNextEntry() != null) {
						is = zis;
						in = new BufferedReader(new InputStreamReader(is));
						continue;
					}
				}
				key = null;
				value = null;
				return false;
			} else {
				return true;
			}
		}
	}

	@Override
	public LongWritable getCurrentKey() {
		return key;
	}

	@Override
	public List<Text> getCurrentValue() {
		return value;
	}

	/**
	 * Get the progress within the split
	 */
	public float getProgress() {
		if (start == end) {
			return 0.0f;
		} else {
			return Math.min(1.0f, (pos - start) / (float) (end - start));
		}
	}

	public synchronized void close() throws IOException {
		if (in != null) {
			in.close();
			in = null;
		}
		if (is != null) {
			is.close();
			is = null;
		}
	}
}
