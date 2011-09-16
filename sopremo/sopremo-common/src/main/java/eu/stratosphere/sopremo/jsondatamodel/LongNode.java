package eu.stratosphere.sopremo.jsondatamodel;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

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

	@Override
	public int getTypePos() {
		return JsonNode.TYPES.LongNode.ordinal();
	}

	@Override
	public void read(DataInput in) throws IOException {
		this.value.read(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		this.value.write(out);
	}

	public static LongNode valueOf(long value) {
		return new LongNode(value);
	}

	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + value.hashCode();
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		LongNode other = (LongNode) obj;
		if (!value.equals(other.value))
			return false;
		return true;
	}

	public Double getValueAsDouble() {
		return Double.valueOf(this.value.getValue());
	}

	public long getLongValue() {
		return this.value.getValue();
	}

}
