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

package org.apache.flink.streaming.api.datastream;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.StreamTransformation;

import static java.util.Objects.requireNonNull;

/**
 * A {@code BroadcastStream} is a stream with {@link org.apache.flink.api.common.state.BroadcastState BroadcastState}.
 * This can be created by any stream using the {@link DataStream#broadcast(MapStateDescriptor)} method and
 * implicitly creates a state where the user can store elements of the created {@code BroadcastStream}.
 * (see {@link BroadcastConnectedStream}).
 *
 * <p>Note that no further operation can be applied to these streams. The only available option is to connect them
 * with a keyed or non-keyed stream, using the {@link KeyedStream#connect(BroadcastStream)} and the
 * {@link DataStream#connect(BroadcastStream)} respectively. Applying these methods will result it a
 * {@link BroadcastConnectedStream} for further processing.
 *
 * @param <T> The type of input/output elements.
 * @param <K> The key type of the elements in the {@link org.apache.flink.api.common.state.BroadcastState BroadcastState}.
 * @param <V> The value type of the elements in the {@link org.apache.flink.api.common.state.BroadcastState BroadcastState}.
 */
@PublicEvolving
public class BroadcastStream<T, K, V> {

	private final StreamExecutionEnvironment environment;

	private final DataStream<T> inputStream;

	/**
	 * The {@link org.apache.flink.api.common.state.StateDescriptor state descriptor} of the
	 * {@link org.apache.flink.api.common.state.BroadcastState broadcast state}. This state
	 * has a {@code key-value} format.
	 */
	private final MapStateDescriptor<K, V> broadcastStateDescriptor;

	protected BroadcastStream(
			final StreamExecutionEnvironment env,
			final DataStream<T> input,
			final MapStateDescriptor<K, V> broadcastStateDescriptor) {

		this.environment = requireNonNull(env);
		this.inputStream = requireNonNull(input);
		this.broadcastStateDescriptor = requireNonNull(broadcastStateDescriptor);
	}

	public TypeInformation<T> getType() {
		return inputStream.getType();
	}

	public <F> F clean(F f) {
		return environment.clean(f);
	}

	public StreamTransformation<T> getTransformation() {
		return inputStream.getTransformation();
	}

	public MapStateDescriptor<K, V> getBroadcastStateDescriptor() {
		return broadcastStateDescriptor;
	}

	public StreamExecutionEnvironment getEnvironment() {
		return environment;
	}
}
