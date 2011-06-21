package eu.stratosphere.pact.example.terasort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.pact.common.type.Key;

public class TeraKey implements Key {

	public static final int KEY_SIZE = 10;

	private final byte[] key = new byte[KEY_SIZE];

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(DataOutput out) throws IOException {

		out.write(this.key);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(DataInput in) throws IOException {

		in.readFully(this.key);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int compareTo(Key arg0) {

		if (!(arg0 instanceof TeraKey)) {
			return Integer.MAX_VALUE;
		}

		final TeraKey tsk = (TeraKey) arg0;

		int diff = 0;
		for (int i = 0; i < KEY_SIZE; ++i) {

			diff = (this.key[i] - tsk.key[i]);
			if (diff != 0) {
				break;
			}
		}

		return diff;
	}

	public void copyToBuffer(final byte[] buf) {
		
		System.arraycopy(this.key, 0, buf, 0, KEY_SIZE);
	}
}
