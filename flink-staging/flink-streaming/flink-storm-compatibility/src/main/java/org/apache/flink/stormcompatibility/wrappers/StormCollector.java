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

/* we do not import
 * --> "org.apache.flink.api.java.tuple.Tuple"
 * or
 * --> "backtype.storm.tuple.Tuple"
 * to avoid confusion
 */

import backtype.storm.spout.ISpoutOutputCollector;
import backtype.storm.task.IOutputCollector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple25;
import org.apache.flink.util.Collector;

import java.util.Collection;
import java.util.List;

/**
 * A {@link StormCollector} is used by {@link AbstractStormSpoutWrapper} and {@link StormBoltWrapper} to provided an
 * Storm compatible output collector to the wrapped spout or bolt, respectively. Additionally, {@link StormCollector}
 * transforms the bolt's output tuples into Flink tuples and emits the created Flink tuples using a Flink
 * {@link Collector}.
 */
class StormCollector<OUT> implements ISpoutOutputCollector, IOutputCollector {

	// The Flink collector
	private final Collector<OUT> flinkCollector;
	// The Flink output tuple of concrete type {@link Tuple1} to {@link Tuple25}
	private final org.apache.flink.api.java.tuple.Tuple outputTuple;
	// The number of attributes of the output tuples. (Determines the concrete type of {@link #outputTuple})
	private final int numberOfAttributes;
	// Is set to {@code true} each time a tuple is emitted
	boolean tupleEmitted = false;

	/**
	 * Instantiates a new {@link StormCollector} that emits Flink tuples to the given Flink collector. If the number of
	 * attributes is specified as zero, any output type is supported. If the number of attributes is between 1 to 25,
	 * the output type is {@link Tuple1} to {@link Tuple25}.
	 *
	 * @param numberOfAttributes
	 * 		The number of attributes of the emitted tuples.
	 * @param flinkCollector
	 * 		The Flink collector to be used.
	 * @throws UnsupportedOperationException
	 * 		if the specified number of attributes is not in the valid range of [0,25]
	 */
	public StormCollector(final int numberOfAttributes, final Collector<OUT> flinkCollector)
			throws UnsupportedOperationException {
		this.numberOfAttributes = numberOfAttributes;
		this.flinkCollector = flinkCollector;

		if (this.numberOfAttributes <= 0) {
			this.outputTuple = null;
		} else if (this.numberOfAttributes <= 25) {
			try {
				this.outputTuple = Tuple.getTupleClass(this.numberOfAttributes).newInstance();
			} catch (final InstantiationException e) {
				throw new RuntimeException(e);
			} catch (final IllegalAccessException e) {
				throw new RuntimeException(e);
			}
		} else {
			throw new UnsupportedOperationException(
					"SimpleStormBoltWrapper can handle not more then 25 attributes, but " + this.numberOfAttributes
							+ " are declared by the given bolt");
		}
	}

	@Override
	public void reportError(final Throwable error) {
		// not sure, if Flink can support this
		throw new UnsupportedOperationException("Not implemented yet");
	}

	@Override
	public List<Integer> emit(final String streamId, final List<Object> tuple, final Object messageId) {
		return this.emitImpl(tuple);
	}

	@Override
	public List<Integer> emit(final String streamId, final Collection<backtype.storm.tuple.Tuple> anchors,
			final List<Object> tuple) {
		return this.emitImpl(tuple);
	}

	@SuppressWarnings("unchecked")
	public List<Integer> emitImpl(
			final List<Object> tuple) {
		if (this.numberOfAttributes > 0) {
			assert (tuple.size() == this.numberOfAttributes);
			for (int i = 0; i < this.numberOfAttributes; ++i) {
				this.outputTuple.setField(tuple.get(i), i);
			}
			this.flinkCollector.collect((OUT) this.outputTuple);
		} else {
			assert (tuple.size() == 1);
			this.flinkCollector.collect((OUT) tuple.get(0));
		}
		this.tupleEmitted = true;

		// TODO
		return null;
	}

	@Override
	public void emitDirect(final int taskId, final String streamId, final List<Object> tuple, final Object messageId) {
		throw new UnsupportedOperationException("Direct emit is not supported by Flink");
	}

	@Override
	public void emitDirect(final int taskId, final String streamId,
			final Collection<backtype.storm.tuple.Tuple> anchors, final List<Object> tuple) {
		throw new UnsupportedOperationException("Direct emit is not supported by Flink");
	}

	@Override
	public void ack(final backtype.storm.tuple.Tuple input) {
		throw new UnsupportedOperationException("Currently, acking/failing is not supported by Flink");
	}

	@Override
	public void fail(final backtype.storm.tuple.Tuple input) {
		throw new UnsupportedOperationException("Currently, acking/failing is not supported by Flink");
	}

}
