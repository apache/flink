package eu.stratosphere.sopremo.jsondatamodel;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class BigIntegerNode extends NumericNode {

	/**
	 * 
	 */
	
	//TODO implement!
	private static final long serialVersionUID = 1758754799197009675L;

	@Override
	public boolean equals(Object o) {
		return false;
	}

	@Override
	public int getTypePos() {
		return 0;
	}

	@Override
	public void read(DataInput in) throws IOException {
	}

	@Override
	public void write(DataOutput out) throws IOException {
	}

}
