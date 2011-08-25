package eu.stratosphere.pact.example.skew;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.nephele.template.InputSplit;

public class NativeGenInputSplit implements InputSplit {

	private int splitNum;
	private int splitCnt;
	
	public NativeGenInputSplit() { }
	
	public NativeGenInputSplit(int splitNum, int splitCnt) {
		this.splitCnt = splitCnt;
		this.splitNum = splitNum;
	}
	
	@Override
	public int getSplitNumber() {
		return this.splitNum;
	}
	
	public int getSplitCount() {
		return this.splitCnt;
	}

	@Override
	public void read(DataInput in) throws IOException {
		this.splitNum = in.readInt();
		this.splitCnt = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.splitNum);
		out.writeInt(this.splitCnt);
	}

}
