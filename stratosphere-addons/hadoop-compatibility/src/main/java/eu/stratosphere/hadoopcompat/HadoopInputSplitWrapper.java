package eu.stratosphere.hadoopcompat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.mapred.JobConf;

import eu.stratosphere.core.io.InputSplit;


public class HadoopInputSplitWrapper implements InputSplit {

	public transient org.apache.hadoop.mapred.InputSplit hadoopInputSplit;
	public JobConf jobConf;
	private int splitNumber;
	private String hadoopInputSplitTypeName;
	
	
	public org.apache.hadoop.mapred.InputSplit getHadoopInputSplit() {
		return hadoopInputSplit;
	}
	
	
	public HadoopInputSplitWrapper() {
		super();
	}
	
	
	public HadoopInputSplitWrapper(org.apache.hadoop.mapred.InputSplit hInputSplit, JobConf jobconf) {
		this.hadoopInputSplit = hInputSplit;
		this.hadoopInputSplitTypeName = hInputSplit.getClass().getCanonicalName();
		this.jobConf=jobconf;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(splitNumber);
		out.writeUTF(hadoopInputSplitTypeName);
		hadoopInputSplit.write(out);
	}

	@Override
	public void read(DataInput in) throws IOException {
		this.splitNumber=in.readInt();
		this.hadoopInputSplitTypeName = in.readUTF();
		if(hadoopInputSplit == null) {
			try {
				Class inputSplit = Class.forName(hadoopInputSplitTypeName );
				this.hadoopInputSplit = (org.apache.hadoop.mapred.InputSplit) WritableFactories.newInstance( inputSplit );
			} catch (Exception e) {
				throw new RuntimeException("Unable to create InputSplit", e);
			}
		}
		this.hadoopInputSplit.readFields(in);
	}

	@Override
	public int getSplitNumber() {
		return this.splitNumber;
	}

	public void setSplitNumber(int splitNumber) {
		this.splitNumber = splitNumber;
	}
	
	public void setHadoopInputSplit(
			org.apache.hadoop.mapred.InputSplit hadoopInputSplit) {
		this.hadoopInputSplit = hadoopInputSplit;
	}
}
