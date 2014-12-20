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

package org.apache.flink.streaming.api.datastream;

import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.SerializationException;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.collector.OutputSelector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.streamvertex.StreamingRuntimeContext;
import org.apache.flink.streaming.state.OperatorState;

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

	protected SingleOutputStreamOperator(StreamExecutionEnvironment environment,
			String operatorType, TypeInformation<OUT> outTypeInfo) {
		super(environment, operatorType, outTypeInfo);
		setBufferTimeout(environment.getBufferTimeout());
	}

	protected SingleOutputStreamOperator(DataStream<OUT> dataStream) {
		super(dataStream);
	}

	@SuppressWarnings("unchecked")
	public <R> SingleOutputStreamOperator<R, ?> setType(TypeInformation<R> outType) {
		jobGraphBuilder.setOutType(id, outType);
		typeInfo = outType;
		return (SingleOutputStreamOperator<R, ?>) this;
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
		try {
			jobGraphBuilder.setOutputSelector(id,
					SerializationUtils.serialize(clean(outputSelector)));

		} catch (SerializationException e) {
			throw new RuntimeException("Cannot serialize OutputSelector");
		}

		return new SplitDataStream<OUT>(this);
	}

	/**
	 * This is a beta feature </br></br> Register an operator state for this
	 * operator by the given name. This name can be used to retrieve the state
	 * during runtime using {@link StreamingRuntimeContext#getState(String)}. To
	 * obtain the {@link StreamingRuntimeContext} from the user-defined function
	 * use the {@link RichFunction#getRuntimeContext()} method.
	 * 
	 * @param name
	 *            The name of the operator state.
	 * @param state
	 *            The state to be registered for this name.
	 * @return The data stream with state registered.
	 */
	protected SingleOutputStreamOperator<OUT, O> registerState(String name, OperatorState<?> state) {
		jobGraphBuilder.addOperatorState(getId(), name, state);
		return this;
	}

	/**
	 * This is a beta feature </br></br> Register operator states for this
	 * operator provided in a map. The registered states can be retrieved during
	 * runtime using {@link StreamingRuntimeContext#getState(String)}. To obtain
	 * the {@link StreamingRuntimeContext} from the user-defined function use
	 * the {@link RichFunction#getRuntimeContext()} method.
	 * 
	 * @param states
	 *            The map containing the states that will be registered.
	 * @return The data stream with states registered.
	 */
	protected SingleOutputStreamOperator<OUT, O> registerState(Map<String, OperatorState<?>> states) {
		for (Entry<String, OperatorState<?>> entry : states.entrySet()) {
			jobGraphBuilder.addOperatorState(getId(), entry.getKey(), entry.getValue());
		}

		return this;
	}

	@SuppressWarnings("unchecked")
	public SingleOutputStreamOperator<OUT, O> partitionBy(int... keypositions) {
		return (SingleOutputStreamOperator<OUT, O>) super.partitionBy(keypositions);
	}

	@SuppressWarnings("unchecked")
	public SingleOutputStreamOperator<OUT, O> partitionBy(String... fields) {
		return (SingleOutputStreamOperator<OUT, O>) super.partitionBy(fields);
	}

	@SuppressWarnings("unchecked")
	public SingleOutputStreamOperator<OUT, O> partitionBy(KeySelector<OUT, ?> keySelector) {
		return (SingleOutputStreamOperator<OUT, O>) super.partitionBy(keySelector);
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
