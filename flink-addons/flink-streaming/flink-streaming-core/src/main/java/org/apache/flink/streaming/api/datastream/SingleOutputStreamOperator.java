/**
 *
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
 *
 */

package org.apache.flink.streaming.api.datastream;

import org.apache.commons.lang3.SerializationException;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.flink.streaming.api.collector.OutputSelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * The SingleOutputStreamOperator represents a user defined transformation
 * applied on a {@link DataStream} with one predefined output type.
 *
 * @param <OUT>
 *            Output type of the operator.
 * @param <O>
 *            Type of the operator.
 */
public class SingleOutputStreamOperator<OUT, O extends SingleOutputStreamOperator<OUT, O>> extends
		DataStream<OUT> {

	protected SingleOutputStreamOperator(StreamExecutionEnvironment environment, String operatorType) {
		super(environment, operatorType);
		setBufferTimeout(environment.getBufferTimeout());
	}

	protected SingleOutputStreamOperator(DataStream<OUT> dataStream) {
		super(dataStream);
	}

	/**
	 * Sets the degree of parallelism for this operator. The degree must be 1 or
	 * more.
	 * 
	 * @param dop
	 *            The degree of parallelism for this operator.
	 * @return The operator with set degree of parallelism.
	 */
	public SingleOutputStreamOperator<OUT, O> setParallelism(int dop) {
		if (dop < 1) {
			throw new IllegalArgumentException("The parallelism of an operator must be at least 1.");
		}
		this.degreeOfParallelism = dop;

		jobGraphBuilder.setParallelism(id, degreeOfParallelism);

		return this;
	}

	/**
	 * Sets the mutability of the operator. If the operator is set to mutable,
	 * the tuples received in the user defined functions, will be reused after
	 * the function call. Setting an operator to mutable reduces garbage
	 * collection overhead and thus increases scalability. Please note that if a
	 * {@link DataStream#batchReduce} or {@link DataStream#windowReduce} is used
	 * as mutable, the user can only iterate through the iterator once in every
	 * invoke.
	 * 
	 * @param isMutable
	 *            The mutability of the operator.
	 * @return The operator with mutability set.
	 */
	public SingleOutputStreamOperator<OUT, O> setMutability(boolean isMutable) {
		jobGraphBuilder.setMutability(id, isMutable);
		return this;
	}

	/**
	 * Sets the maximum time frequency (ms) for the flushing of the output
	 * buffer. By default the output buffers flush only when they are full.
	 * 
	 * @param timeoutMillis
	 *            The maximum time between two output flushes.
	 * @return The operator with buffer timeout set.
	 */
	public SingleOutputStreamOperator<OUT, O> setBufferTimeout(long timeoutMillis) {
		jobGraphBuilder.setBufferTimeout(id, timeoutMillis);
		return this;
	}

	/**
	 * Operator used for directing tuples to specific named outputs using an
	 * {@link OutputSelector}. Calling this method on an operator creates a new
	 * {@link SplitDataStream}.
	 * 
	 * @param outputSelector
	 *            The user defined {@link OutputSelector} for directing the
	 *            tuples.
	 * @return The {@link SplitDataStream}
	 */
	public SplitDataStream<OUT> split(OutputSelector<OUT> outputSelector) {
		return split(outputSelector, null);
	}

	/**
	 * Operator used for directing tuples to specific named outputs using an
	 * {@link OutputSelector}. Calling this method on an operator creates a new
	 * {@link SplitDataStream}.
	 * 
	 * @param outputSelector
	 *            The user defined {@link OutputSelector} for directing the
	 *            tuples.
	 * @param outputNames
	 *            An array of all the output names to be used for selectAll
	 * @return The {@link SplitDataStream}
	 */
	public SplitDataStream<OUT> split(OutputSelector<OUT> outputSelector, String[] outputNames) {
		try {
			jobGraphBuilder.setOutputSelector(id, SerializationUtils.serialize(outputSelector));

		} catch (SerializationException e) {
			throw new RuntimeException("Cannot serialize OutputSelector");
		}

		return new SplitDataStream<OUT>(this, outputNames);
	}

	@SuppressWarnings("unchecked")
	public SingleOutputStreamOperator<OUT, O> partitionBy(int keyposition) {
		return (SingleOutputStreamOperator<OUT, O>) super.partitionBy(keyposition);
	}

	@SuppressWarnings("unchecked")
	public SingleOutputStreamOperator<OUT, O> broadcast() {
		return (SingleOutputStreamOperator<OUT, O>) super.broadcast();
	}

	@SuppressWarnings("unchecked")
	public SingleOutputStreamOperator<OUT, O> shuffle() {
		return (SingleOutputStreamOperator<OUT, O>) super.shuffle();
	}

	@SuppressWarnings("unchecked")
	public SingleOutputStreamOperator<OUT, O> forward() {
		return (SingleOutputStreamOperator<OUT, O>) super.forward();
	}

	@SuppressWarnings("unchecked")
	public SingleOutputStreamOperator<OUT, O> distribute() {
		return (SingleOutputStreamOperator<OUT, O>) super.distribute();
	}

	@Override
	protected SingleOutputStreamOperator<OUT, O> copy() {
		return new SingleOutputStreamOperator<OUT, O>(this);
	}

}
