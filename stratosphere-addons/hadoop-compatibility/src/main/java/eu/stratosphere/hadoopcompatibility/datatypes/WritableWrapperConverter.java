package eu.stratosphere.hadoopcompatibility.datatypes;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import eu.stratosphere.types.Record;
import eu.stratosphere.types.Value;

public class WritableWrapperConverter<K extends WritableComparable, V extends Writable> implements HadoopTypeConverter<K,V> {
	private static final long serialVersionUID = 1L;

	@Override
	public void convert(Record stratosphereRecord, K hadoopKey, V hadoopValue) {
		stratosphereRecord.setField(0, convertKey(hadoopKey));
		stratosphereRecord.setField(1, convertValue(hadoopValue));
	}
	
	private final Value convertKey(K in) {
		return new WritableComparableWrapper<K>(in);
	}
	
	private final Value convertValue(V in) {
		return new WritableWrapper<V>(in);
	}
}