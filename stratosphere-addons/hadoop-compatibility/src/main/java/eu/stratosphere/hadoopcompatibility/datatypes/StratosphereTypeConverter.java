package eu.stratosphere.hadoopcompatibility.datatypes;

import eu.stratosphere.types.Record;

import java.io.Serializable;

/**
 * An interface describing a class that is able to
 * convert Stratosphere's Record into Hadoop types model.
 *
 * The converter must be Serializable.
 *
 * Stratosphere provides a DefaultStratosphereTypeConverter. Custom implementations should
 * chain the type converters.
 */
public interface StratosphereTypeConverter<K,V> extends Serializable {

	/**
	 * Convert a Stratosphere type to a Hadoop type.
	 */
	public K convertKey(Record stratosphereRecord);

	public V convertValue(Record stratosphereRecord);
}
