package eu.stratosphere.hadoopcompat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.FileSplitSerDeUtil;
import org.apache.hadoop.mapred.JobConf;

import eu.stratosphere.core.io.InputSplit;


public class HadoopInputSplitWrapper implements InputSplit {

	public transient org.apache.hadoop.mapred.InputSplit hadoopInputSplit;
	public JobConf jobConf;
	
	
	public org.apache.hadoop.mapred.InputSplit getHadoopInputSplit() {
		return hadoopInputSplit;
	}
	
	private int splitNumber;

	public HadoopInputSplitWrapper(org.apache.hadoop.mapred.InputSplit hInputSplit, JobConf jobconf) {
		this.hadoopInputSplit = hInputSplit;
		this.jobConf=jobconf;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(splitNumber);
//		this.jobConf.write(out);
		hadoopInputSplit.write(out);
	}

	@Override
	public void read(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.splitNumber=in.readInt();
//		this.jobConf=new JobConf();
//		this.jobConf=this.jobConf.readFields(in);
		if(this.hadoopInputSplit==null)
		{
//			SerializationFactory factory = new SerializationFactory(jobConf);
//		    Deserializer<org.apache.hadoop.mapred.InputSplit> deserializer =  (Deserializer<InputSplit>) factory.getDeserializer(this.hadoopInputSplit);
//		    deserializer.open(inFile);
//		    T split = deserializer.deserialize(null);
		}
		this.hadoopInputSplit = FileSplitSerDeUtil.deSerialize(in);
//		this.hadoopInputSplit.readFields(in);
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
	
	public HadoopInputSplitWrapper() {
		super();
	}

}
