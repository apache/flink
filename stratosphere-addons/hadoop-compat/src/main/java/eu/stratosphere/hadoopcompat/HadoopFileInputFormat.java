package eu.stratosphere.hadoopcompat;

import java.io.IOException;

import org.apache.hadoop.mapred.InputFormat;

import eu.stratosphere.api.common.io.FileInputFormat;
import eu.stratosphere.core.fs.FileInputSplit;

public class HadoopFileInputFormat<OT> extends FileInputFormat<OT> {

	private FileInputSplit split;
	private InputFormat hadoopInputFormat;
	
	public HadoopFileInputFormat(InputFormat hadoopInputFormat)
	{
		this.hadoopInputFormat=hadoopInputFormat;
	}
	@Override
	public void open(FileInputSplit split) throws IOException {
		super.open(split);
	}
	@Override
	public boolean reachedEnd() throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean nextRecord(OT record) throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

}
