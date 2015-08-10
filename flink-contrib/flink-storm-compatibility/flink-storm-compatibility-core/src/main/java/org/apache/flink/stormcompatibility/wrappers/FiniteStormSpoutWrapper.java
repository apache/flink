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

/**
 * A {@link FiniteStormSpoutWrapper} is an {@link AbstractStormSpoutWrapper} that calls the wrapped
 * {@link FiniteStormSpout}'s {@link FiniteStormSpout#nextTuple()} method until {@link
 * FiniteStormSpout#reachedEnd()} is true.
 */
public class FiniteStormSpoutWrapper<OUT> extends AbstractStormSpoutWrapper<OUT> {
	private static final long serialVersionUID = -218340336648247605L;

	private FiniteStormSpout finiteSpout;

	/**
	 * Instantiates a new {@link FiniteStormSpoutWrapper} that wraps the given Storm {@link
	 * FiniteStormSpout spout} such that it can be used within a Flink streaming program. The
	 * output
	 * type will be one of {@link Tuple1} to {@link Tuple25} depending on the spout's declared
	 * number of attributes.
	 *
	 * @param spout
	 * 		The Storm {@link FiniteStormSpout spout} to be used. @throws
	 * 		IllegalArgumentException If
	 * 		the number of declared output attributes is not with range [1;25].
	 */
	public FiniteStormSpoutWrapper(FiniteStormSpout spout)
			throws IllegalArgumentException {
		super(spout);
		this.finiteSpout = spout;
	}

	/**
	 * Instantiates a new {@link FiniteStormSpoutWrapper} that wraps the given Storm {@link
	 * FiniteStormSpout spout} such that it can be used within a Flink streaming program. The
	 * output
	 * type can be any type if parameter {@code rawOutput} is {@code true} and the spout's
	 * number of
	 * declared output tuples is 1. If {@code rawOutput} is {@code false} the output type will be
	 * one of {@link Tuple1} to {@link Tuple25} depending on the spout's declared number of
	 * attributes.
	 *
	 * @param spout
	 * 		The Storm {@link FiniteStormSpout spout} to be used.
	 * @param rawOutput
	 * 		Set to {@code true} if a single attribute output stream, should not be of type {@link
	 * 		Tuple1} but be of a raw type.
	 * @throws IllegalArgumentException
	 * 		If {@code rawOuput} is {@code true} and the number of declared output attributes is
	 * 		not 1
	 * 		or if {@code rawOuput} is {@code false} and the number of declared output attributes
	 * 		is not
	 * 		with range [1;25].
	 */
	public FiniteStormSpoutWrapper(final FiniteStormSpout spout, final boolean rawOutput)
			throws IllegalArgumentException {
		super(spout, rawOutput);
		this.finiteSpout = spout;
	}

	/**
	 * Calls the {@link FiniteStormSpout#nextTuple()} method until {@link
	 * FiniteStormSpout#reachedEnd()} is true or {@link FiniteStormSpout#cancel()} is called.
	 */
	@Override
	protected void execute() {
		while (super.isRunning && !finiteSpout.reachedEnd()) {
			finiteSpout.nextTuple();
		}
	}

}
