/**
 * Copyright 2014 Marcelo Elias Del Valle
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.mapreduce.lib.input;

import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContextImpl;
import org.apache.hadoop.mapred.TaskAttemptContextImpl;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CSVNLineInputFormatTest {

    @Test public void shouldReturnListsAsRecords() throws Exception {
        JobConf conf = createConfig();
        TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());

        CSVNLineInputFormat inputFormat = new CSVNLineInputFormat();
        List<InputSplit> actualSplits = inputFormat.getSplits(new JobContextImpl(conf, new JobID()) {

        });
        RecordReader<LongWritable, List<Text>> recordReader = inputFormat.createRecordReader(actualSplits.get(0), context);

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

    private JobConf createConfig() {
        JobConf conf = new JobConf();

        conf.setStrings("mapreduce.input.fileinputformat.inputdir", "./fixtures/teste2.csv");
        conf.set(CSVLineRecordReader.FORMAT_DELIMITER, "\"");
        conf.set(CSVLineRecordReader.FORMAT_SEPARATOR, ",");
        conf.setInt(CSVNLineInputFormat.LINES_PER_MAP, 40000);
        conf.setBoolean(CSVLineRecordReader.IS_ZIPFILE, false);

        return conf;
    }
}
