package eu.stratosphere.hadoopcompat.datatypes;

import java.io.Serializable;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import eu.stratosphere.types.Record;


/**
 * An interface describing a class that is able to 
 * convert Hadoop types into Stratosphere's Record model.
 * 
 * The converter must be Serializable.
 * 
 * Stratosphere provides a DefaultHadoopTypeConverter. Custom implementations should
 * chain the type converters.
 */
public interface HadoopTypeConverter<K extends WritableComparable<?>, V extends Writable> extends Serializable {
	
	/**
	 * Convert a Hadoop type to a Stratosphere type.
	 */
	public void convert(Record stratosphereRecord, K hadoopKey, V hadoopValue);
}
