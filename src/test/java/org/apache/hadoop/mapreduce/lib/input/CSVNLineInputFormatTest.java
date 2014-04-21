package org.apache.hadoop.mapreduce.lib.input;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class CSVNLineInputFormatTest {

    @Test
    public void shouldReturnListsAsRecords() throws Exception {
        Configuration conf = createConfig();
        TaskAttemptContext context = new TaskAttemptContext(conf, new TaskAttemptID());

        CSVNLineInputFormat inputFormat = new CSVNLineInputFormat();
        List<InputSplit> actualSplits = inputFormat.getSplits(new JobContext(conf, new JobID()));
        RecordReader<LongWritable, List<Text>> recordReader =
                inputFormat.createRecordReader(actualSplits.get(0), context);

        recordReader.initialize(actualSplits.get(0), context);

        recordReader.nextKeyValue();
        List<Text> firstLineValue = recordReader.getCurrentValue();

        assertEquals("Joe Demo", firstLineValue.get(0).toString());
        assertEquals("2 Demo Street,\nDemoville,\nAustralia. 2615", firstLineValue.get(1).toString());
        assertEquals("joe@someaddress.com", firstLineValue.get(2).toString());

        recordReader.nextKeyValue();
        List<Text> secondLineValue = recordReader.getCurrentValue();

        assertEquals("Jim Sample", secondLineValue.get(0).toString());
        assertEquals("", secondLineValue.get(1).toString());
        assertEquals("jim@sample.com", secondLineValue.get(2).toString());

        recordReader.nextKeyValue();
        List<Text> thirdLineValue = recordReader.getCurrentValue();

        assertEquals("Jack Example", thirdLineValue.get(0).toString());
        assertEquals("1 Example Street, Exampleville, Australia.\n2615", thirdLineValue.get(1).toString());
        assertEquals("jack@example.com", thirdLineValue.get(2).toString());
    }

    private Configuration createConfig() {
        Configuration conf = new Configuration();

        conf.setStrings("mapred.input.dir", "./fixtures/teste2.csv");
        conf.set(CSVLineRecordReader.FORMAT_DELIMITER, "\"");
        conf.set(CSVLineRecordReader.FORMAT_SEPARATOR, ",");
        conf.setInt(CSVNLineInputFormat.LINES_PER_MAP, 40000);
        conf.setBoolean(CSVLineRecordReader.IS_ZIPFILE, false);

        return conf;
    }
}
