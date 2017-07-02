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

package org.apache.flink.storm.wrappers;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple0;
import org.apache.flink.api.java.tuple.Tuple25;
import org.apache.flink.storm.util.SplitStreamType;

import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

/**
 * A {@link AbstractStormCollector} transforms Storm tuples to Flink tuples.
 */
abstract class AbstractStormCollector<OUT> {

	/** Flink output tuple of concrete type {@link Tuple0} to {@link Tuple25} per output stream. */
	protected final HashMap<String, Tuple> outputTuple = new HashMap<String, Tuple>();
	/** Flink split tuple. Used, if multiple output streams are declared. */
	private final SplitStreamType<Object> splitTuple = new SplitStreamType<Object>();
	/**
	 * The number of attributes of the output tuples per stream. (Determines the concrete type of {@link #outputTuple}).
	 * If {@link #numberOfAttributes} is zero, {@link #outputTuple} is not used and "raw" data type is used.
	 */
	protected final HashMap<String, Integer> numberOfAttributes;
	/** Indicates of multiple output stream are declared and thus {@link SplitStreamType} must be used as output. */
	private final boolean split;
	/** The ID of the producer task. */
	private final int taskId;
	/** Is set to {@code true} each time a tuple is emitted. */
	boolean tupleEmitted = false;

	/**
	 * Instantiates a new {@link AbstractStormCollector} that emits Flink tuples via {@link #doEmit(Object)}. If the
	 * number of attributes is negative, any output type is supported (ie, raw type). If the number of attributes is
	 * between 0 and 25, the output type is {@link Tuple0} to {@link Tuple25}, respectively.
	 *
	 * @param numberOfAttributes
	 *            The number of attributes of the emitted tuples per output stream.
	 * @param taskId
	 *            The ID of the producer task (negative value for unknown).
	 * @throws UnsupportedOperationException
	 *             if the specified number of attributes is greater than 25 or taskId support is enabled for a raw
	 *             stream
	 */
	AbstractStormCollector(final HashMap<String, Integer> numberOfAttributes, final int taskId)
			throws UnsupportedOperationException {
		assert (numberOfAttributes != null);

		this.numberOfAttributes = numberOfAttributes;
		this.split = this.numberOfAttributes.size() > 1;
		this.taskId = taskId;

		for (Entry<String, Integer> outputStream : numberOfAttributes.entrySet()) {
			int numAtt = outputStream.getValue();

			if (this.taskId >= 0) {
				if (numAtt < 0) {
					throw new UnsupportedOperationException(
							"Task ID transmission not supported for raw streams: "
									+ outputStream.getKey());
				}
				++numAtt;
			}

			if (numAtt > 25) {
				if (this.taskId >= 0) {
					throw new UnsupportedOperationException(
							"Flink cannot handle more then 25 attributes, but 25 (24 plus 1 for produer task ID) "
									+ " are declared for stream '" + outputStream.getKey() + "' by the given bolt.");
				} else {
					throw new UnsupportedOperationException(
							"Flink cannot handle more then 25 attributes, but " + numAtt
							+ " are declared for stream '" + outputStream.getKey() + "' by the given bolt.");
				}
			} else if (numAtt >= 0) {
				try {
					this.outputTuple.put(outputStream.getKey(),
							org.apache.flink.api.java.tuple.Tuple.getTupleClass(numAtt)
							.newInstance());
				} catch (final InstantiationException e) {
					throw new RuntimeException(e);
				} catch (final IllegalAccessException e) {
					throw new RuntimeException(e);
				}

			}
		}
	}

	/**
	 * Transforms a Storm tuple into a Flink tuple of type {@code OUT} and emits this tuple via {@link #doEmit(Object)}
	 * to the specified output stream.
	 *
	 * @param The
	 *            The output stream id.
	 * @param tuple
	 *            The Storm tuple to be emitted.
	 * @return the return value of {@link #doEmit(Object)}
	 */
	@SuppressWarnings("unchecked")
	protected final List<Integer> tansformAndEmit(final String streamId, final List<Object> tuple) {
		List<Integer> taskIds;

		int numAtt = this.numberOfAttributes.get(streamId);
		int taskIdIdx = numAtt;
		if (this.taskId >= 0 && numAtt < 0) {
			numAtt = 1;
			taskIdIdx = 0;
		}
		if (numAtt >= 0) {
			assert (tuple.size() == numAtt);
			Tuple out = this.outputTuple.get(streamId);
			for (int i = 0; i < numAtt; ++i) {
				out.setField(tuple.get(i), i);
			}
			if (this.taskId >= 0) {
				out.setField(this.taskId, taskIdIdx);
			}
			if (this.split) {
				this.splitTuple.streamId = streamId;
				this.splitTuple.value = out;

				taskIds = doEmit((OUT) this.splitTuple);
			} else {
				taskIds = doEmit((OUT) out);
			}

		} else {
			assert (tuple.size() == 1);
			if (this.split) {
				this.splitTuple.streamId = streamId;
				this.splitTuple.value = tuple.get(0);

				taskIds = doEmit((OUT) this.splitTuple);
			} else {
				taskIds = doEmit((OUT) tuple.get(0));
			}
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
