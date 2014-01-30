package eu.stratosphere.hadoopcompat.datatypes;

import org.apache.hadoop.io.WritableComparable;

import eu.stratosphere.types.Key;

public class WritableComparableWrapper<T extends WritableComparable<?>> extends WritableWrapper<T> implements Key {
	private static final long serialVersionUID = 1L;
	
	public WritableComparableWrapper() {
		super();
	}
	
	public WritableComparableWrapper(T toWrap) {
		super(toWrap);
	}

	@Override
	public int compareTo(Key o) {
		return ((WritableComparable) super.value()).compareTo( ((WritableComparableWrapper<T>) o).value() );
	}
}
