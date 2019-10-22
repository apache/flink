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
import org.apache.flink.api.java.typeutils.InputTypeConfigurable;
import org.apache.flink.streaming.api.functions.sink.OutputFormatSinkFunction;
import org.apache.flink.streaming.api.functions.source.InputFormatSourceFunction;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Simple factory which just wrap existed {@link StreamOperator}.
 *
 * @param <OUT> The output type of the operator
 */
@Internal
public class SimpleOperatorFactory<OUT> implements StreamOperatorFactory<OUT> {

	private final StreamOperator<OUT> operator;

	/**
	 * Create a SimpleOperatorFactory from existed StreamOperator.
	 */
	@SuppressWarnings("unchecked")
	public static <OUT> SimpleOperatorFactory<OUT> of(StreamOperator<OUT> operator) {
		if (operator == null) {
			return null;
		} else if (operator instanceof StreamSource &&
				((StreamSource) operator).getUserFunction() instanceof InputFormatSourceFunction) {
			return new SimpleInputFormatOperatorFactory<OUT>((StreamSource) operator);
		} else if (operator instanceof StreamSink &&
			((StreamSink) operator).getUserFunction() instanceof OutputFormatSinkFunction) {
			return new SimpleOutputFormatOperatorFactory<>((StreamSink) operator);
		} else if (operator instanceof AbstractUdfStreamOperator) {
			return new SimpleUdfStreamOperatorFactory<OUT>((AbstractUdfStreamOperator) operator);
		} else {
			return new SimpleOperatorFactory<>(operator);
		}
	}

	protected SimpleOperatorFactory(StreamOperator<OUT> operator) {
		this.operator = checkNotNull(operator);
	}

	public StreamOperator<OUT> getOperator() {
		return operator;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T extends StreamOperator<OUT>> T createStreamOperator(StreamTask<?, ?> containingTask,
			StreamConfig config, Output<StreamRecord<OUT>> output) {
		if (operator instanceof SetupableStreamOperator) {
			((SetupableStreamOperator) operator).setup(containingTask, config, output);
		}
		return (T) operator;
	}

	@Override
	public void setChainingStrategy(ChainingStrategy strategy) {
		operator.setChainingStrategy(strategy);
	}

	@Override
	public ChainingStrategy getChainingStrategy() {
		return operator.getChainingStrategy();
	}

	@Override
	public boolean isStreamSource() {
		return operator instanceof StreamSource;
	}

	@Override
	public boolean isOutputTypeConfigurable() {
		return operator instanceof OutputTypeConfigurable;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void setOutputType(TypeInformation<OUT> type, ExecutionConfig executionConfig) {
		((OutputTypeConfigurable<OUT>) operator).setOutputType(type, executionConfig);
	}

	@Override
	public boolean isInputTypeConfigurable() {
		return operator instanceof InputTypeConfigurable;
	}

	@Override
	public void setInputType(TypeInformation<?> type, ExecutionConfig executionConfig) {
		((InputTypeConfigurable) operator).setInputType(type, executionConfig);
	}

	@Override
	public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
		return operator.getClass();
	}
}
