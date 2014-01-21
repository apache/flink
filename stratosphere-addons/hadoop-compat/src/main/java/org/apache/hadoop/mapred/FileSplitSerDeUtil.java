package org.apache.hadoop.mapred;

import java.io.DataInput;
import java.io.IOException;

public class FileSplitSerDeUtil {
	
	public static InputSplit deSerialize(DataInput in)
	{
		InputSplit is = new FileSplit();
		try {
			is.readFields(in);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return is;
	}

}
