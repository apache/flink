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

import org.apache.flink.api.connector.sink.Committer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The state fo the {@link AbstractStreamingCommitterOperator}.
 *
 * @param <CommT> The committable type of the {@link Committer}.
 */
final class StreamingCommitterState<CommT> {

	private final List<CommT> committables;

	StreamingCommitterState(List<CommT> committables) {
		this.committables = checkNotNull(committables);
	}

	StreamingCommitterState(NavigableMap<Long, List<CommT>> committablesPerCheckpoint) {
		committables = new ArrayList<>();
		for (Map.Entry<Long, List<CommT>> item : committablesPerCheckpoint.entrySet()) {
			committables.addAll(item.getValue());
		}
	}

	public List<CommT> getCommittables() {
		return committables;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		StreamingCommitterState<?> that = (StreamingCommitterState<?>) o;
		return committables.equals(that.committables);
	}

	@Override
	public int hashCode() {
		return Objects.hash(committables);
	}
}
