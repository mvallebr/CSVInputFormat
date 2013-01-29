package org.apache.hadoop.mapreduce.lib.input;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class ArrayListTextWritable extends ArrayList<Text> implements Writable {
	private static final long serialVersionUID = -6737762624115237320L;


	@Override
	public void write(DataOutput dataoutput) throws IOException {
		dataoutput.writeInt(this.size());
		for (Text element : this) {
			element.write(dataoutput);
		}
	}

	@Override
	public void readFields(DataInput datainput) throws IOException {
		int count = datainput.readInt();
		for (int i = 0; i < count; i++) {
			try {
				Text obj = Text.class.newInstance();
				obj.readFields(datainput);
				this.add(obj);
			} catch (InstantiationException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			}
		}
	}

}
