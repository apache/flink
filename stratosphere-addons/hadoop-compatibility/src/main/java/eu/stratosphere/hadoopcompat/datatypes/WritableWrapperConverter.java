package eu.stratosphere.hadoopcompat.datatypes;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import eu.stratosphere.types.Key;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.Value;

public class WritableWrapperConverter implements HadoopTypeConverter {
	private static final long serialVersionUID = 1L;

	@Override
	public void convert(Record stratosphereRecord, Object hadoopKey, Object hadoopValue) {
		stratosphereRecord.setField(0, convert(hadoopKey));
		Value val =  convert(hadoopValue);
		stratosphereRecord.setField(1, val);
	}
	
	private Value convert(Object in) {
		if(!(in instanceof Writable)) {
			throw new RuntimeException("Found element that is not Writable. "
					+ "It is "+in.getClass().getCanonicalName());
		}
		if(in instanceof WritableComparable) {
			return new WritableComparableWrapper((WritableComparable) in);
		}
		return new WritableWrapper((Writable) in);
	}
	
}