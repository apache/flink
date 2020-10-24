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
 *
 */

package org.apache.flink.api.connector.sink;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.metrics.MetricGroup;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;

/**
 * This interface lets the sink developer build a simple sink topology, which could guarantee the exactly once
 * semantics in both batch and stream execution mode if there is a {@link Committer} or {@link GlobalCommitter}.
 * 1. The {@link Writer} is responsible for producing the committable.
 * 2. The {@link Committer} is responsible for committing a single committable.
 * 3. The {@link GlobalCommitter} is responsible for committing an aggregated committable, which we call the global
 *    committable. The {@link GlobalCommitter} is always executed with a parallelism of 1.
 * Note: Developers need to ensure the idempotence of {@link Committer} and {@link GlobalCommitter}.
 *
 * @param <InputT>        The type of the sink's input
 * @param <CommT>         The type of information needed to commit data staged by the sink
 * @param <WriterStateT>  The type of the writer's state
 * @param <GlobalCommT>   The type of the aggregated committable
 */
@Experimental
public interface Sink<InputT, CommT, WriterStateT, GlobalCommT> extends Serializable {

	/**
	 * Create a {@link Writer}.
	 * @param context the runtime context.
	 * @param states the writer's state.
	 * @return A sink writer.
	 */
	Writer<InputT, CommT, WriterStateT> createWriter(InitContext context, List<WriterStateT> states);

	/**
	 * Creates a {@link Committer}.
	 */
	Optional<Committer<CommT>> createCommitter();

	/**
	 * Creates a {@link GlobalCommitter}.
	 */
	Optional<GlobalCommitter<CommT, GlobalCommT>> createGlobalCommitter();

	/**
	 * Returns the serializer of the committable type.
	 */
	Optional<SimpleVersionedSerializer<CommT>> getCommittableSerializer();

	/**
	 * Returns the serializer of the aggregated committable type.
	 */
	Optional<SimpleVersionedSerializer<GlobalCommT>> getGlobalCommittableSerializer();

	/**
	 * Return the serializer of the writer's state type.
	 */
	Optional<SimpleVersionedSerializer<WriterStateT>> getWriterStateSerializer();

	/**
	 * The interface exposes some runtime info for creating a {@link Writer}.
	 */
	interface InitContext {

		/**
		 * @return The id of task where the writer is.
		 */
		int getSubtaskId();

		/**
		 * @return The metric group this writer belongs to.
		 */
		MetricGroup metricGroup();
	}
}
