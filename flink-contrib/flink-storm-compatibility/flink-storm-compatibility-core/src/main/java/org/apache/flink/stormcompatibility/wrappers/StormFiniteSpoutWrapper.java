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

import backtype.storm.topology.IRichSpout;

import org.apache.flink.api.java.tuple.Tuple0;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple25;

import com.google.common.collect.Sets;

/**
 * A {@link StormFiniteSpoutWrapper} is an {@link AbstractStormSpoutWrapper} that calls {@link IRichSpout#nextTuple()
 * nextTuple()} for finite number of times before
 * {@link #run(org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext)} returns. The number of
 * {@code nextTuple()} calls can be specified as a certain number of invocations or can be undefined. In the undefined
 * case, the {@code run(...)} method return if no record was emitted to the output collector for the first time.
 */
public class StormFiniteSpoutWrapper<OUT> extends AbstractStormSpoutWrapper<OUT> {
	private static final long serialVersionUID = 3883246587044801286L;

	/** The number of {@link IRichSpout#nextTuple()} calls */
	private int numberOfInvocations;

	/**
	 * Instantiates a new {@link StormFiniteSpoutWrapper} that calls the {@link IRichSpout#nextTuple() nextTuple()}
	 * method of the given Storm {@link IRichSpout spout} as long as it emits records to the output collector. The
	 * output type will be one of {@link Tuple0} to {@link Tuple25} depending on the spout's declared number of
	 * attributes.
	 * 
	 * @param spout
	 *            The Storm {@link IRichSpout spout} to be used.
	 * @throws IllegalArgumentException
	 *             If the number of declared output attributes is not with range [0;25].
	 */
	public StormFiniteSpoutWrapper(final IRichSpout spout) throws IllegalArgumentException {
		this(spout, (Collection<String>) null, -1);
	}

	/**
	 * Instantiates a new {@link StormFiniteSpoutWrapper} that calls the {@link IRichSpout#nextTuple() nextTuple()}
	 * method of the given Storm {@link IRichSpout spout} {@code numberOfInvocations} times. The output type will be one
	 * of {@link Tuple0} to {@link Tuple25} depending on the spout's declared number of attributes.
	 * 
	 * @param spout
	 *            The Storm {@link IRichSpout spout} to be used.
	 * @param numberOfInvocations
	 *            The number of calls to {@link IRichSpout#nextTuple()}.
	 * @throws IllegalArgumentException
	 *             If the number of declared output attributes is not with range [0;25].
	 */
	public StormFiniteSpoutWrapper(final IRichSpout spout, final int numberOfInvocations)
			throws IllegalArgumentException {
		this(spout, (Collection<String>) null, numberOfInvocations);
	}

	/**
	 * Instantiates a new {@link StormFiniteSpoutWrapper} that calls the {@link IRichSpout#nextTuple() nextTuple()}
	 * method of the given Storm {@link IRichSpout spout} as long as it emits records to the output collector. The
	 * output type can be any type if parameter {@code rawOutput} is {@code true} and the spout's number of declared
	 * output tuples is 1. If {@code rawOutput} is {@code false} the output type will be one of {@link Tuple0} to
	 * {@link Tuple25} depending on the spout's declared number of attributes.
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
	public StormFiniteSpoutWrapper(final IRichSpout spout, final String[] rawOutputs)
			throws IllegalArgumentException {
		this(spout, Sets.newHashSet(rawOutputs), -1);
	}

	/**
	 * Instantiates a new {@link StormFiniteSpoutWrapper} that calls the {@link IRichSpout#nextTuple() nextTuple()}
	 * method of the given Storm {@link IRichSpout spout} as long as it emits records to the output collector. The
	 * output type can be any type if parameter {@code rawOutput} is {@code true} and the spout's number of declared
	 * output tuples is 1. If {@code rawOutput} is {@code false} the output type will be one of {@link Tuple0} to
	 * {@link Tuple25} depending on the spout's declared number of attributes.
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
	public StormFiniteSpoutWrapper(final IRichSpout spout, final Collection<String> rawOutputs)
			throws IllegalArgumentException {
		this(spout, rawOutputs, -1);
	}

	/**
	 * Instantiates a new {@link StormFiniteSpoutWrapper} that calls the {@link IRichSpout#nextTuple() nextTuple()}
	 * method of the given Storm {@link IRichSpout spout} {@code numberOfInvocations} times. The output type can be any
	 * type if parameter {@code rawOutput} is {@code true} and the spout's number of declared output tuples is 1. If
	 * {@code rawOutput} is {@code false} the output type will be one of {@link Tuple0} to {@link Tuple25} depending on
	 * the spout's declared number of attributes.
	 * 
	 * @param spout
	 *            The Storm {@link IRichSpout spout} to be used.
	 * @param rawOutputs
	 *            Contains stream names if a single attribute output stream, should not be of type {@link Tuple1} but be
	 *            of a raw type.
	 * @param numberOfInvocations
	 *            The number of calls to {@link IRichSpout#nextTuple()}.
	 * @throws IllegalArgumentException
	 *             If {@code rawOuput} is {@code true} and the number of declared output attributes is not 1 or if
	 *             {@code rawOuput} is {@code false} and the number of declared output attributes is not with range
	 *             [0;25].
	 */
	public StormFiniteSpoutWrapper(final IRichSpout spout, final String[] rawOutputs,
			final int numberOfInvocations) throws IllegalArgumentException {
		super(spout, Sets.newHashSet(rawOutputs));
		this.numberOfInvocations = numberOfInvocations;
	}

	/**
	 * Instantiates a new {@link StormFiniteSpoutWrapper} that calls the {@link IRichSpout#nextTuple() nextTuple()}
	 * method of the given Storm {@link IRichSpout spout} {@code numberOfInvocations} times. The output type can be any
	 * type if parameter {@code rawOutput} is {@code true} and the spout's number of declared output tuples is 1. If
	 * {@code rawOutput} is {@code false} the output type will be one of {@link Tuple0} to {@link Tuple25} depending on
	 * the spout's declared number of attributes.
	 * 
	 * @param spout
	 *            The Storm {@link IRichSpout spout} to be used.
	 * @param rawOutputs
	 *            Contains stream names if a single attribute output stream, should not be of type {@link Tuple1} but be
	 *            of a raw type.
	 * @param numberOfInvocations
	 *            The number of calls to {@link IRichSpout#nextTuple()}.
	 * @throws IllegalArgumentException
	 *             If {@code rawOuput} is {@code true} and the number of declared output attributes is not 1 or if
	 *             {@code rawOuput} is {@code false} and the number of declared output attributes is not with range
	 *             [0;25].
	 */
	public StormFiniteSpoutWrapper(final IRichSpout spout, final Collection<String> rawOutputs,
			final int numberOfInvocations) throws IllegalArgumentException {
		super(spout, rawOutputs);
		this.numberOfInvocations = numberOfInvocations;
	}

	/**
	 * Calls {@link IRichSpout#nextTuple()} for the given number of times.
	 */
	@Override
	protected void execute() {
		if (this.numberOfInvocations >= 0) {
			while ((--this.numberOfInvocations >= 0) && super.isRunning) {
				super.spout.nextTuple();
			}
		} else {
			do {
				super.collector.tupleEmitted = false;
				super.spout.nextTuple();
			} while (super.collector.tupleEmitted && super.isRunning);
		}
	}

}
