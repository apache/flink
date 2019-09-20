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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;

import java.io.Serializable;

/**
 * A factory to create {@link StreamOperator}.
 *
 * @param <OUT> The output type of the operator
 */
@Internal
public interface StreamOperatorFactory<OUT> extends Serializable {

	/**
	 * Create the operator. Sets access to the context and the output.
	 */
	<T extends StreamOperator<OUT>> T createStreamOperator(
			StreamTask<?, ?> containingTask, StreamConfig config, Output<StreamRecord<OUT>> output);

	/**
	 * Set the chaining strategy for operator factory.
	 */
	void setChainingStrategy(ChainingStrategy strategy);

	/**
	 * Get the chaining strategy of operator factory.
	 */
	ChainingStrategy getChainingStrategy();

	/**
	 * Is this factory for {@link StreamSource}.
	 */
	default boolean isStreamSource() {
		return false;
	}

	/**
	 * If the stream operator need access to the output type information at {@link StreamGraph}
	 * generation. This can be useful for cases where the output type is specified by the returns
	 * method and, thus, after the stream operator has been created.
	 */
	default boolean isOutputTypeConfigurable() {
		return false;
	}

	/**
	 * Is called by the {@link StreamGraph#addOperator} method when the {@link StreamGraph} is
	 * generated. The method is called with the output {@link TypeInformation} which is also used
	 * for the {@link StreamTask} output serializer.
	 *
	 * @param type Output type information of the {@link StreamTask}
	 * @param executionConfig Execution configuration
	 */
	default void setOutputType(TypeInformation<OUT> type, ExecutionConfig executionConfig) {}

	/**
	 * If the stream operator need to be configured with the data type they will operate on.
	 */
	default boolean isInputTypeConfigurable() {
		return false;
	}

	/**
	 * Is called by the {@link StreamGraph#addOperator} method when the {@link StreamGraph} is
	 * generated.
	 *
	 * @param type The data type of the input.
	 * @param executionConfig The execution config for this parallel execution.
	 */
	default void setInputType(TypeInformation<?> type, ExecutionConfig executionConfig) {}

	/**
	 * Returns the runtime class of the stream operator.
	 */
	Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader);
}
