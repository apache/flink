/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.operators.sink;

import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;

import javax.annotation.Nullable;

/**
 * A {@link org.apache.flink.streaming.api.operators.StreamOperatorFactory} for {@link
 * StatefulSinkWriterOperator}.
 *
 * @param <InputT> The input type of the {@link SinkWriter}.
 * @param <CommT> The committable type of the {@link SinkWriter}.
 * @param <WriterStateT> The type of the {@link SinkWriter Writer's} state.
 */
public final class StatefulSinkWriterOperatorFactory<InputT, CommT, WriterStateT> extends AbstractSinkWriterOperatorFactory<InputT, CommT> {

	private final Sink<InputT, CommT, WriterStateT, ?> sink;

	@Nullable
	private final String previousSinkStateName;

	public StatefulSinkWriterOperatorFactory(Sink<InputT, CommT, WriterStateT, ?> sink) {
		this(sink, null);
	}

	public StatefulSinkWriterOperatorFactory(
			Sink<InputT, CommT, WriterStateT, ?> sink,
			@Nullable String previousSinkStateName) {
		this.sink = sink;
		this.previousSinkStateName = previousSinkStateName;
	}

	@Override
	AbstractSinkWriterOperator<InputT, CommT> createWriterOperator(ProcessingTimeService processingTimeService) {
		return new StatefulSinkWriterOperator<>(
				previousSinkStateName,
				processingTimeService,
				sink,
				sink.getWriterStateSerializer().get());
	}

	@Override
	public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
		return StatefulSinkWriterOperator.class;
	}
}
