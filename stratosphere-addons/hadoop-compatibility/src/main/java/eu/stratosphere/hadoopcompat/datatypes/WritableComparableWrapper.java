package eu.stratosphere.hadoopcompat.datatypes;

import org.apache.hadoop.io.WritableComparable;

import eu.stratosphere.types.Key;

public class WritableComparableWrapper extends WritableWrapper implements Key {
	private static final long serialVersionUID = 1L;
	
	public WritableComparableWrapper() {
		super();
	}
	
	public WritableComparableWrapper(WritableComparable toWrap) {
		super(toWrap);
	}

	@Override
	public int compareTo(Key o) {
		return ((WritableComparable) super.value()).compareTo( ((WritableComparableWrapper) o).value() );
	}
}
