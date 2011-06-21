package eu.stratosphere.pact.example.terasort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.pact.common.type.Value;

public final class TeraValue implements Value {

	public static final int VALUE_SIZE = 89;

	private final byte[] value = new byte[VALUE_SIZE];

	public TeraValue(final byte[] srcBuf) {
		System.arraycopy(srcBuf, TeraKey.KEY_SIZE, this.value, 0, VALUE_SIZE);
	}

	public TeraValue() {
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(DataOutput out) throws IOException {

		out.write(this.value);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(DataInput in) throws IOException {

		in.readFully(this.value);
	}

	public void copyToBuffer(final byte[] buf) {

		System.arraycopy(this.value, 0, buf, TeraKey.KEY_SIZE, VALUE_SIZE);
	}

	@Override
	public String toString() {

		return new String(this.value);
	}
}
