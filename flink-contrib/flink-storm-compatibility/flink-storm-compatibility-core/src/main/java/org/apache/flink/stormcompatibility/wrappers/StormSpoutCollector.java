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

import backtype.storm.spout.ISpoutOutputCollector;

import org.apache.flink.api.java.tuple.Tuple0;
import org.apache.flink.api.java.tuple.Tuple25;
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;

import java.util.HashMap;
import java.util.List;

/**
 * A {@link StormSpoutCollector} is used by {@link AbstractStormSpoutWrapper} to provided an Storm
 * compatible output collector to the wrapped spout. It transforms the emitted Storm tuples into
 * Flink tuples and emits them via the provide {@link SourceContext} object.
 */
class StormSpoutCollector<OUT> extends AbstractStormCollector<OUT> implements ISpoutOutputCollector {

	/** The Flink source context object */
	private final SourceContext<OUT> flinkContext;

	/**
	 * Instantiates a new {@link StormSpoutCollector} that emits Flink tuples to the given Flink source context. If the
	 * number of attributes is specified as zero, any output type is supported. If the number of attributes is between 0
	 * to 25, the output type is {@link Tuple0} to {@link Tuple25}, respectively.
	 * 
	 * @param numberOfAttributes
	 *            The number of attributes of the emitted tuples.
	 * @param flinkContext
	 *            The Flink source context to be used.
	 * @throws UnsupportedOperationException
	 *             if the specified number of attributes is greater than 25
	 */
	public StormSpoutCollector(final HashMap<String, Integer> numberOfAttributes,
			final SourceContext<OUT> flinkContext) throws UnsupportedOperationException {
		super(numberOfAttributes);
		assert (flinkContext != null);
		this.flinkContext = flinkContext;
	}

	@Override
	protected List<Integer> doEmit(final OUT flinkTuple) {
		this.flinkContext.collect(flinkTuple);
		// TODO
		return null;
	}

	@Override
	public void reportError(final Throwable error) {
		// not sure, if Flink can support this
		throw new UnsupportedOperationException("Not implemented yet");
	}

	@Override
	public List<Integer> emit(final String streamId, final List<Object> tuple, final Object messageId) {
		return this.tansformAndEmit(streamId, tuple);
	}


	@Override
	public void emitDirect(final int taskId, final String streamId, final List<Object> tuple, final Object messageId) {
		throw new UnsupportedOperationException("Direct emit is not supported by Flink");
	}

}
