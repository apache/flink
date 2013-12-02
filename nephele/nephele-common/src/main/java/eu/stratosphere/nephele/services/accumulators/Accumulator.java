package eu.stratosphere.nephele.services.accumulators;

import java.io.Serializable;

import eu.stratosphere.nephele.io.IOReadableWritable;

/**
 * Interface for custom accumulator objects. Data are written to in a UDF and
 * merged by the system at the end of the job. The result can be read at the end
 * of the job from the calling client. Inspired by Hadoop/MapReduce counters.<br>
 * <br>
 * 
 * The type added to the accumulator might differ from the type returned. This
 * is the case e.g. for a set-accumulator: We add single objects, but the result
 * is a set of objects.
 * 
 * @param <V>
 *            Type of values that are added to the accumulator
 * @param <R>
 *            Type of the accumulator result as it will be reported to the
 *            client
 */
public interface Accumulator<V, R> extends IOReadableWritable, Serializable {

	/**
	 * @param value
	 *            The value to add to the accumulator object
	 */
	void add(V value);

	/**
	 * @return local The local value from the current UDF context
	 */
	R getLocalValue();

	/**
	 * Reset the local value. This only affects the current UDF context.
	 */
	void resetLocal();

	/**
	 * Used by system internally to merge the collected parts of an accumulator
	 * at the end of the job.
	 * 
	 * @param other
	 *            reference to accumulator to merge in
	 * @return Reference to this (for efficiency), after data from other were
	 *         merged in
	 */
	void merge(Accumulator<V, R> other);

}
