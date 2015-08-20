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

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.api.transformations.SinkTransformation;

/**
 * A Stream Sink. This is used for emitting elements from a streaming topology.
 *
 * @param <T> The type of the elements in the Stream
 */
public class DataStreamSink<T> {

	SinkTransformation<T> transformation;

	@SuppressWarnings("unchecked")
	protected DataStreamSink(DataStream<T> inputStream, StreamSink<T> operator) {
		this.transformation = new SinkTransformation<T>(inputStream.getTransformation(), "Unnamed", operator, inputStream.getExecutionEnvironment().getParallelism());
	}

	/**
	 * Returns the transformation that contains the actual sink operator of this sink.
	 */
	public SinkTransformation<T> getTransformation() {
		return transformation;
	}

	/**
	 * Sets the name of this sink. This name is
	 * used by the visualization and logging during runtime.
	 *
	 * @return The named sink.
	 */
	public DataStreamSink<T> name(String name) {
		transformation.setName(name);
		return this;
	}

	/**
	 * Sets the parallelism for this sink. The degree must be higher than zero.
	 *
	 * @param parallelism The parallelism for this sink.
	 * @return The sink with set parallelism.
	 */
	public DataStreamSink<T> setParallelism(int parallelism) {
		transformation.setParallelism(parallelism);
		return this;
	}

	/**
	 * Turns off chaining for this operator so thread co-location will not be
	 * used as an optimization.
	 *
	 * <p>
	 * Chaining can be turned off for the whole
	 * job by {@link org.apache.flink.streaming.api.environment.StreamExecutionEnvironment#disableOperatorChaining()}
	 * however it is not advised for performance considerations.
	 *
	 * @return The sink with chaining disabled
	 */
	public DataStreamSink<T> disableChaining() {
		this.transformation.setChainingStrategy(AbstractStreamOperator.ChainingStrategy.NEVER);
		return this;
	}
}
