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

import org.apache.flink.api.common.ExecutionConfig.GlobalJobParameters;
import org.apache.flink.api.common.functions.StoppableFunction;
import org.apache.flink.api.java.tuple.Tuple0;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple25;
import org.apache.flink.storm.util.FiniteSpout;
import org.apache.flink.storm.util.StormConfig;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;

import org.apache.storm.generated.StormTopology;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;

import java.util.Collection;
import java.util.HashMap;

import static java.util.Arrays.asList;

/**
 * A {@link SpoutWrapper} wraps an {@link IRichSpout} in order to execute it within a Flink Streaming program. It
 * takes the spout's output tuples and transforms them into Flink tuples of type {@code OUT} (see
 * {@link SpoutCollector} for supported types).<br>
 * <br>
 * Per default, {@link SpoutWrapper} calls the wrapped spout's {@link IRichSpout#nextTuple() nextTuple()} method in
 * an infinite loop.<br>
 * Alternatively, {@link SpoutWrapper} can call {@link IRichSpout#nextTuple() nextTuple()} for a finite number of
 * times and terminate automatically afterwards (for finite input streams). The number of {@code nextTuple()} calls can
 * be specified as a certain number of invocations or can be undefined. In the undefined case, {@link SpoutWrapper}
 * terminates if no record was emitted to the output collector for the first time during a call to
 * {@link IRichSpout#nextTuple() nextTuple()}.<br>
 * If the given spout implements {@link FiniteSpout} interface and {@link #numberOfInvocations} is not provided or
 * is {@code null}, {@link SpoutWrapper} calls {@link IRichSpout#nextTuple() nextTuple()} method until
 * {@link FiniteSpout#reachedEnd()} returns true.
 */
public final class SpoutWrapper<OUT> extends RichParallelSourceFunction<OUT> implements StoppableFunction {
	private static final long serialVersionUID = -218340336648247605L;

	/** Number of attributes of the spouts's output tuples per stream. */
	private final HashMap<String, Integer> numberOfAttributes;
	/** The wrapped {@link IRichSpout spout}. */
	private final IRichSpout spout;
	/** The name of the spout. */
	private final String name;
	/** Indicates, if the source is still running or was canceled. */
	private volatile boolean isRunning = true;
	/** The number of {@link IRichSpout#nextTuple()} calls. */
	private Integer numberOfInvocations; // do not use int -> null indicates an infinite loop
	/** The original Storm topology. */
	private StormTopology stormTopology;

	/**
	 * Instantiates a new {@link SpoutWrapper} that calls the {@link IRichSpout#nextTuple() nextTuple()} method of
	 * the given {@link IRichSpout spout} in an infinite loop. The output type will be one of {@link Tuple0} to
	 * {@link Tuple25} depending on the spout's declared number of attributes.
	 *
	 * @param spout
	 *            The {@link IRichSpout spout} to be used.
	 * @throws IllegalArgumentException
	 *             If the number of declared output attributes is not with range [0;25].
	 */
	public SpoutWrapper(final IRichSpout spout) throws IllegalArgumentException {
		this(spout, (Collection<String>) null, null);
	}

	/**
	 * Instantiates a new {@link SpoutWrapper} that calls the {@link IRichSpout#nextTuple() nextTuple()} method of
	 * the given {@link IRichSpout spout} a finite number of times. The output type will be one of {@link Tuple0} to
	 * {@link Tuple25} depending on the spout's declared number of attributes.
	 *
	 * @param spout
	 *            The {@link IRichSpout spout} to be used.
	 * @param numberOfInvocations
	 *            The number of calls to {@link IRichSpout#nextTuple()}. If value is negative, {@link SpoutWrapper}
	 *            terminates if no tuple was emitted for the first time. If value is {@code null}, finite invocation is
	 *            disabled.
	 * @throws IllegalArgumentException
	 *             If the number of declared output attributes is not with range [0;25].
	 */
	public SpoutWrapper(final IRichSpout spout, final Integer numberOfInvocations)
			throws IllegalArgumentException {
		this(spout, (Collection<String>) null, numberOfInvocations);
	}

	/**
	 * Instantiates a new {@link SpoutWrapper} that calls the {@link IRichSpout#nextTuple() nextTuple()} method of
	 * the given {@link IRichSpout spout} in an infinite loop. The output type can be any type if parameter
	 * {@code rawOutput} is {@code true} and the spout's number of declared output tuples is 1. If {@code rawOutput} is
	 * {@code false} the output type will be one of {@link Tuple0} to {@link Tuple25} depending on the spout's declared
	 * number of attributes.
	 *
	 * @param spout
	 *            The {@link IRichSpout spout} to be used.
	 * @param rawOutputs
	 *            Contains stream names if a single attribute output stream, should not be of type {@link Tuple1} but be
	 *            of a raw type. (Can be {@code null}.)
	 * @throws IllegalArgumentException
	 *             If {@code rawOutput} is {@code true} and the number of declared output attributes is not 1 or if
	 *             {@code rawOutput} is {@code false} and the number of declared output attributes is not with range
	 *             [0;25].
	 */
	public SpoutWrapper(final IRichSpout spout, final String[] rawOutputs)
			throws IllegalArgumentException {
		this(spout, asList(rawOutputs), null);
	}

	/**
	 * Instantiates a new {@link SpoutWrapper} that calls the {@link IRichSpout#nextTuple() nextTuple()} method of
	 * the given {@link IRichSpout spout} a finite number of times. The output type can be any type if parameter
	 * {@code rawOutput} is {@code true} and the spout's number of declared output tuples is 1. If {@code rawOutput} is
	 * {@code false} the output type will be one of {@link Tuple0} to {@link Tuple25} depending on the spout's declared
	 * number of attributes.
	 *
	 * @param spout
	 *            The {@link IRichSpout spout} to be used.
	 * @param rawOutputs
	 *            Contains stream names if a single attribute output stream, should not be of type {@link Tuple1} but be
	 *            of a raw type. (Can be {@code null}.)
	 * @param numberOfInvocations
	 *            The number of calls to {@link IRichSpout#nextTuple()}. If value is negative, {@link SpoutWrapper}
	 *            terminates if no tuple was emitted for the first time. If value is {@code null}, finite invocation is
	 *            disabled.
	 * @throws IllegalArgumentException
	 *             If {@code rawOutput} is {@code true} and the number of declared output attributes is not 1 or if
	 *             {@code rawOutput} is {@code false} and the number of declared output attributes is not with range
	 *             [0;25].
	 */
	public SpoutWrapper(final IRichSpout spout, final String[] rawOutputs,
			final Integer numberOfInvocations) throws IllegalArgumentException {
		this(spout, asList(rawOutputs), numberOfInvocations);
	}

	/**
	 * Instantiates a new {@link SpoutWrapper} that calls the {@link IRichSpout#nextTuple() nextTuple()} method of
	 * the given {@link IRichSpout spout} in an infinite loop. The output type can be any type if parameter
	 * {@code rawOutput} is {@code true} and the spout's number of declared output tuples is 1. If {@code rawOutput} is
	 * {@code false} the output type will be one of {@link Tuple0} to {@link Tuple25} depending on the spout's declared
	 * number of attributes.
	 *
	 * @param spout
	 *            The {@link IRichSpout spout} to be used.
	 * @param rawOutputs
	 *            Contains stream names if a single attribute output stream, should not be of type {@link Tuple1} but be
	 *            of a raw type. (Can be {@code null}.)
	 * @throws IllegalArgumentException
	 *             If {@code rawOutput} is {@code true} and the number of declared output attributes is not 1 or if
	 *             {@code rawOutput} is {@code false} and the number of declared output attributes is not with range
	 *             [0;25].
	 */
	public SpoutWrapper(final IRichSpout spout, final Collection<String> rawOutputs)
			throws IllegalArgumentException {
		this(spout, rawOutputs, null);
	}

	/**
	 * Instantiates a new {@link SpoutWrapper} that calls the {@link IRichSpout#nextTuple() nextTuple()} method of
	 * the given {@link IRichSpout spout} a finite number of times. The output type can be any type if parameter
	 * {@code rawOutput} is {@code true} and the spout's number of declared output tuples is 1. If {@code rawOutput} is
	 * {@code false} the output type will be one of {@link Tuple0} to {@link Tuple25} depending on the spout's declared
	 * number of attributes.
	 *
	 * @param spout
	 *            The {@link IRichSpout spout} to be used.
	 * @param rawOutputs
	 *            Contains stream names if a single attribute output stream, should not be of type {@link Tuple1} but be
	 *            of a raw type. (Can be {@code null}.)
	 * @param numberOfInvocations
	 *            The number of calls to {@link IRichSpout#nextTuple()}. If value is negative, {@link SpoutWrapper}
	 *            terminates if no tuple was emitted for the first time. If value is {@code null}, finite invocation is
	 *            disabled.
	 * @throws IllegalArgumentException
	 *             If {@code rawOutput} is {@code true} and the number of declared output attributes is not 1 or if
	 *             {@code rawOutput} is {@code false} and the number of declared output attributes is not with range
	 *             [0;25].
	 */
	public SpoutWrapper(final IRichSpout spout, final Collection<String> rawOutputs,
			final Integer numberOfInvocations) throws IllegalArgumentException {
		this(spout, null, rawOutputs, numberOfInvocations);
	}

	/**
	 * Instantiates a new {@link SpoutWrapper} that calls the {@link IRichSpout#nextTuple() nextTuple()} method of
	 * the given {@link IRichSpout spout} a finite number of times. The output type can be any type if parameter
	 * {@code rawOutput} is {@code true} and the spout's number of declared output tuples is 1. If {@code rawOutput} is
	 * {@code false} the output type will be one of {@link Tuple0} to {@link Tuple25} depending on the spout's declared
	 * number of attributes.
	 *
	 * @param spout
	 *            The {@link IRichSpout spout} to be used.
	 * @param name
	 *            The name of the spout.
	 * @param rawOutputs
	 *            Contains stream names if a single attribute output stream, should not be of type {@link Tuple1} but be
	 *            of a raw type. (Can be {@code null}.)
	 * @param numberOfInvocations
	 *            The number of calls to {@link IRichSpout#nextTuple()}. If value is negative, {@link SpoutWrapper}
	 *            terminates if no tuple was emitted for the first time. If value is {@code null}, finite invocation is
	 *            disabled.
	 * @throws IllegalArgumentException
	 *             If {@code rawOutput} is {@code true} and the number of declared output attributes is not 1 or if
	 *             {@code rawOutput} is {@code false} and the number of declared output attributes is not with range
	 *             [0;25].
	 */
	public SpoutWrapper(final IRichSpout spout, final String name, final Collection<String> rawOutputs,
			final Integer numberOfInvocations) throws IllegalArgumentException {
		this.spout = spout;
		this.name = name;
		this.numberOfAttributes = WrapperSetupHelper.getNumberOfAttributes(spout, rawOutputs);
		this.numberOfInvocations = numberOfInvocations;
	}

	/**
	 * Sets the original Storm topology.
	 *
	 * @param stormTopology
	 *            The original Storm topology.
	 */
	public void setStormTopology(StormTopology stormTopology) {
		this.stormTopology = stormTopology;
	}

	@Override
	public final void run(final SourceContext<OUT> ctx) throws Exception {
		final GlobalJobParameters config = super.getRuntimeContext().getExecutionConfig()
				.getGlobalJobParameters();
		StormConfig stormConfig = new StormConfig();

		if (config != null) {
			if (config instanceof StormConfig) {
				stormConfig = (StormConfig) config;
			} else {
				stormConfig.putAll(config.toMap());
			}
		}

		final TopologyContext stormTopologyContext = WrapperSetupHelper.createTopologyContext(
				(StreamingRuntimeContext) super.getRuntimeContext(), this.spout, this.name,
				this.stormTopology, stormConfig);

		SpoutCollector<OUT> collector = new SpoutCollector<OUT>(this.numberOfAttributes,
				stormTopologyContext.getThisTaskId(), ctx);

		this.spout.open(stormConfig, stormTopologyContext, new SpoutOutputCollector(collector));
		this.spout.activate();

		if (numberOfInvocations == null) {
			if (this.spout instanceof FiniteSpout) {
				final FiniteSpout finiteSpout = (FiniteSpout) this.spout;

				while (this.isRunning && !finiteSpout.reachedEnd()) {
					finiteSpout.nextTuple();
				}
			} else {
				while (this.isRunning) {
					this.spout.nextTuple();
				}
			}
		} else {
			int counter = this.numberOfInvocations;
			if (counter >= 0) {
				while ((--counter >= 0) && this.isRunning) {
					this.spout.nextTuple();
				}
			} else {
				do {
					collector.tupleEmitted = false;
					this.spout.nextTuple();
				} while (collector.tupleEmitted && this.isRunning);
			}
		}
	}

	/**
	 * {@inheritDoc}
	 *
	 * <p>Sets the {@link #isRunning} flag to {@code false}.
	 */
	@Override
	public void cancel() {
		this.isRunning = false;
	}

	/**
	 * {@inheritDoc}
	 *
	 * <p>Sets the {@link #isRunning} flag to {@code false}.
	 */
	@Override
	public void stop() {
		this.isRunning = false;
	}

	@Override
	public void close() throws Exception {
		this.spout.close();
	}

}
