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

import java.util.Collection;
import java.util.HashMap;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.topology.IRichSpout;

import org.apache.flink.api.java.tuple.Tuple0;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple25;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.runtime.tasks.StreamingRuntimeContext;

/**
 * A {@link AbstractStormSpoutWrapper} wraps an {@link IRichSpout} in order to execute the Storm bolt within a Flink
 * Streaming program. It takes the spout's output tuples and transforms them into Flink tuples of type {@code OUT} (see
 * {@link StormSpoutCollector} for supported types).<br />
 * <br />
 * <strong>CAUTION: currently, only simple spouts are supported! (ie, spouts that do not use the Storm configuration
 * <code>Map</code> or <code>TopologyContext</code> that is provided by the spouts's <code>prepare(..)</code> method.
 * Furthermore, ack and fail back calls as well as tuple IDs are not supported so far.</strong>
 */
public abstract class AbstractStormSpoutWrapper<OUT> extends RichParallelSourceFunction<OUT> {
	private static final long serialVersionUID = 4993283609095408765L;

	/**
	 * Number of attributes of the bolt's output tuples per stream.
	 */
	private final HashMap<String, Integer> numberOfAttributes;
	/**
	 * The wrapped Storm {@link IRichSpout spout}.
	 */
	protected final IRichSpout spout;
	/**
	 * The wrapper of the given Flink collector.
	 */
	protected StormSpoutCollector<OUT> collector;
	/**
	 * Indicates, if the source is still running or was canceled.
	 */
	protected volatile boolean isRunning = true;

	/**
	 * Instantiates a new {@link AbstractStormSpoutWrapper} that wraps the given Storm {@link IRichSpout spout} such
	 * that it can be used within a Flink streaming program. The output type will be one of {@link Tuple0} to
	 * {@link Tuple25} depending on the spout's declared number of attributes.
	 *
	 * @param spout
	 * 		The Storm {@link IRichSpout spout} to be used.
	 * @throws IllegalArgumentException
	 * 		If the number of declared output attributes is not with range [0;25].
	 */
	public AbstractStormSpoutWrapper(final IRichSpout spout) throws IllegalArgumentException {
		this(spout, null);
	}

	/**
	 * Instantiates a new {@link AbstractStormSpoutWrapper} that wraps the given Storm {@link IRichSpout spout} such
	 * that it can be used within a Flink streaming program. The output type can be any type if parameter
	 * {@code rawOutput} is {@code true} and the spout's number of declared output tuples is 1. If {@code rawOutput} is
	 * {@code false} the output type will be one of {@link Tuple0} to {@link Tuple25} depending on the spout's declared
	 * number of attributes.
	 * 
	 * @param spout
	 *            The Storm {@link IRichSpout spout} to be used.
	 * @param rawOutputs
	 *            Contains stream names if a single attribute output stream, should not be of type {@link Tuple1} but be
	 *            of a raw type.
	 * @throws IllegalArgumentException
	 *             If {@code rawOuput} is {@code true} and the number of declared output attributes is not 1 or if
	 *             {@code rawOuput} is {@code false} and the number of declared output attributes is not with range
	 *             [0;25].
	 */
	public AbstractStormSpoutWrapper(final IRichSpout spout,
			final Collection<String> rawOutputs)
					throws IllegalArgumentException {
		this.spout = spout;
		this.numberOfAttributes = StormWrapperSetupHelper.getNumberOfAttributes(spout, rawOutputs);
	}

	@Override
	public final void run(final SourceContext<OUT> ctx) throws Exception {
		this.collector = new StormSpoutCollector<OUT>(this.numberOfAttributes, ctx);
		this.spout.open(null,
				StormWrapperSetupHelper
				.convertToTopologyContext((StreamingRuntimeContext) super.getRuntimeContext(), true),
				new SpoutOutputCollector(this.collector));
		this.spout.activate();
		this.execute();
	}

	/**
	 * Needs to be implemented to call the given Spout's {@link IRichSpout#nextTuple() nextTuple()} method. This method
	 * might use a {@code while(true)}-loop to emit an infinite number of tuples.
	 */
	protected abstract void execute();

	/**
	 * {@inheritDoc}
	 * <p/>
	 * Sets the {@link #isRunning} flag to {@code false}.
	 */
	@Override
	public void cancel() {
		this.isRunning = false;
	}

	@Override
	public void close() throws Exception {
		this.spout.close();
	}

}
