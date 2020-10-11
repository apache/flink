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
import org.apache.flink.api.connector.sink.Writer;
import org.apache.flink.streaming.api.operators.StreamOperator;

/**
 * A {@link org.apache.flink.streaming.api.operators.StreamOperatorFactory} for {@link
 * StatelessWriterOperator}.
 *
 * @param <InputT> The input type of the {@link Writer}.
 * @param <CommT> The committable type of the {@link Writer}.
 */
public final class StatelessWriterOperatorFactory<InputT, CommT> extends AbstractWriterOperatorFactory<InputT, CommT> {

	private final Sink<InputT, CommT, ?, ?> sink;

	public StatelessWriterOperatorFactory(Sink<InputT, CommT, ?, ?> sink) {
		this.sink = sink;
	}

	@Override
	AbstractWriterOperator<InputT, CommT> createWriterOperator() {
		return new StatelessWriterOperator<>(sink);
	}

	@Override
	public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
		return StatelessWriterOperator.class;
	}
}
