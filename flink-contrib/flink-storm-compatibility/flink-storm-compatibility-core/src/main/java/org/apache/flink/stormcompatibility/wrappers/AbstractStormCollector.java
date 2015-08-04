/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.stormcompatibility.wrappers;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple25;
import java.util.List;

/**
 * A {@link AbstractStormCollector} transforms Storm tuples to Flink tuples.
 */
abstract class AbstractStormCollector<OUT> {

	/**
	 * Flink output tuple of concrete type {@link Tuple1} to {@link Tuple25}.
	 */
	protected final Tuple outputTuple;
	/**
	 * The number of attributes of the output tuples. (Determines the concrete type of
	 * {@link #outputTuple}). If {@link #numberOfAttributes} is zero, {@link #outputTuple} is not
	 * used and "raw" data type is used.
	 */
	protected final int numberOfAttributes;
	/**
	 * Is set to {@code true} each time a tuple is emitted.
	 */
	boolean tupleEmitted = false;

	/**
	 * Instantiates a new {@link AbstractStormCollector} that emits Flink tuples via
	 * {@link #doEmit(Object)}. If the number of attributes is specified as zero, any output type is
	 * supported. If the number of attributes is between 1 to 25, the output type is {@link Tuple1}
	 * to {@link Tuple25}.
	 * 
	 * @param numberOfAttributes
	 * 		The number of attributes of the emitted tuples.
	 * @throws UnsupportedOperationException
	 * 		if the specified number of attributes is not in the valid range of [0,25]
	 */
	public AbstractStormCollector(final int numberOfAttributes) throws UnsupportedOperationException {
		this.numberOfAttributes = numberOfAttributes;

		if (this.numberOfAttributes <= 0) {
			this.outputTuple = null;
		} else if (this.numberOfAttributes <= 25) {
			try {
				this.outputTuple = org.apache.flink.api.java.tuple.Tuple
						.getTupleClass(this.numberOfAttributes).newInstance();
			} catch (final InstantiationException e) {
				throw new RuntimeException(e);
			} catch (final IllegalAccessException e) {
				throw new RuntimeException(e);
			}
		} else {
			throw new UnsupportedOperationException(
					"Flink cannot handle more then 25 attributes, but "
					+ this.numberOfAttributes + " are declared by the given bolt");
		}
	}

	/**
	 * Transforms a Storm tuple into a Flink tuple of type {@code OUT} and emits this tuple via
	 * {@link #doEmit(Object)}.
	 * 
	 * @param tuple
	 * 		The Storm tuple to be emitted.
	 * @return the return value of {@link #doEmit(Object)}
	 */
	@SuppressWarnings("unchecked")
	protected final List<Integer> tansformAndEmit(final List<Object> tuple) {
		List<Integer> taskIds;
		if (this.numberOfAttributes > 0) {
			assert (tuple.size() == this.numberOfAttributes);
			for (int i = 0; i < this.numberOfAttributes; ++i) {
				this.outputTuple.setField(tuple.get(i), i);
			}
			taskIds = doEmit((OUT) this.outputTuple);
		} else {
			assert (tuple.size() == 1);
			taskIds = doEmit((OUT) tuple.get(0));
		}
		this.tupleEmitted = true;

		return taskIds;
	}

	/**
	 * Emits a Flink tuple.
	 * 
	 * @param flinkTuple
	 * 		The tuple to be emitted.
	 * @return the IDs of the tasks this tuple was sent to
	 */
	protected abstract List<Integer> doEmit(OUT flinkTuple);

}
