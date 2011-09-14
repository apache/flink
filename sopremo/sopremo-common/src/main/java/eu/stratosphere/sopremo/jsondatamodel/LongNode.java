package eu.stratosphere.sopremo.jsondatamodel;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactLong;

public class LongNode extends NumericNode {

	/**
	 * 
	 */
	private static final long serialVersionUID = 8594695207002513755L;

	protected final PactLong value;

	public LongNode(long value) {
		this.value = new PactLong(value);
	}

	// TODO implement!

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

	public static JsonNode valueOf(long value) {
		return new LongNode(value);
	}

}
