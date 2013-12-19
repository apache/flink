/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.api.common.accumulators;

import java.io.Serializable;

import eu.stratosphere.core.io.IOReadableWritable;

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
