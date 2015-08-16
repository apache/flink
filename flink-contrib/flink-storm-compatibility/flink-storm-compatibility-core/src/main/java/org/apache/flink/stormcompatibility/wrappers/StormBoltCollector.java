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

import backtype.storm.task.IOutputCollector;
import backtype.storm.tuple.Tuple;

import org.apache.flink.api.java.tuple.Tuple0;
import org.apache.flink.api.java.tuple.Tuple25;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.util.Collector;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;

/**
 * A {@link StormBoltCollector} is used by {@link StormBoltWrapper} to provided an Storm compatible
 * output collector to the wrapped bolt. It transforms the emitted Storm tuples into Flink tuples
 * and emits them via the provide {@link Output} object.
 */
class StormBoltCollector<OUT> extends AbstractStormCollector<OUT> implements IOutputCollector {

	/** The Flink output Collector */
	private final Collector<OUT> flinkOutput;

	/**
	 * Instantiates a new {@link StormBoltCollector} that emits Flink tuples to the given Flink output object. If the
	 * number of attributes is negative, any output type is supported (ie, raw type). If the number of attributes is
	 * between 0 and 25, the output type is {@link Tuple0} to {@link Tuple25}, respectively.
	 * 
	 * @param numberOfAttributes
	 *            The number of attributes of the emitted tuples per output stream.
	 * @param flinkOutput
	 *            The Flink output object to be used.
	 * @throws UnsupportedOperationException
	 *             if the specified number of attributes is greater than 25
	 */
	public StormBoltCollector(final HashMap<String, Integer> numberOfAttributes,
			final Collector<OUT> flinkOutput) throws UnsupportedOperationException {
		super(numberOfAttributes);
		assert (flinkOutput != null);
		this.flinkOutput = flinkOutput;
	}

	@Override
	protected List<Integer> doEmit(final OUT flinkTuple) {
		this.flinkOutput.collect(flinkTuple);
		// TODO
		return null;
	}

	@Override
	public void reportError(final Throwable error) {
		// not sure, if Flink can support this
		throw new UnsupportedOperationException("Not implemented yet");
	}

	@Override
	public List<Integer> emit(final String streamId, final Collection<Tuple> anchors, final List<Object> tuple) {
		return this.tansformAndEmit(streamId, tuple);
	}

	@Override
	public void emitDirect(final int taskId, final String streamId, final Collection<Tuple> anchors, final List<Object> tuple) {
		throw new UnsupportedOperationException("Direct emit is not supported by Flink");
	}

	@Override
	public void ack(final Tuple input) {
		throw new UnsupportedOperationException("Currently, acking/failing is not supported by Flink");
	}

	@Override
	public void fail(final Tuple input) {
		throw new UnsupportedOperationException("Currently, acking/failing is not supported by Flink");
	}

}
