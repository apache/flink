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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.annotation.Internal;

import java.io.Serializable;

/**
 * The state checkpointed by a {@link org.apache.flink.streaming.api.operators.AbstractStreamOperator}.
 * This state consists of any combination of those three:
 * <ul>
 *     <li>The state of the stream operator, if it implements the Checkpointed interface.</li>
 *     <li>The state of the user function, if it implements the Checkpointed interface.</li>
 *     <li>The key/value state of the operator, if it executes on a KeyedDataStream.</li>
 * </ul>
 */
@Internal
public class StreamOperatorState implements Serializable {
	private static final long serialVersionUID = 1603204675128946014L;

	private final StreamOperatorPartitionedState partitionedState;
	private final StreamOperatorNonPartitionedState nonPartitionedState;

	public StreamOperatorState(StreamOperatorPartitionedState partitionedState, StreamOperatorNonPartitionedState nonPartitionedState) {
		this.partitionedState = partitionedState;
		this.nonPartitionedState = nonPartitionedState;
	}

	public StreamOperatorPartitionedState getPartitionedState() {
		return partitionedState;
	}

	public StreamOperatorNonPartitionedState getNonPartitionedState() {
		return nonPartitionedState;
	}
}
