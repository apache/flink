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

package org.apache.flink.runtime.io.network.api;

import java.io.IOException;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.event.RuntimeEvent;

/**
 * Checkpoint barriers are used to align checkpoints throughout the streaming topology. The
 * barriers are emitted by the sources when instructed to do so by the JobManager. When
 * operators receive a CheckpointBarrier on one of its inputs, it knows that this is the point 
 * between the pre-checkpoint and post-checkpoint data.
 * 
 * <p>Once an operator has received a checkpoint barrier from all its input channels, it
 * knows that a certain checkpoint is complete. It can trigger the operator specific checkpoint
 * behavior and broadcast the barrier to downstream operators.</p>
 * 
 * <p>Depending on the semantic guarantees, may hold off post-checkpoint data until the checkpoint
 * is complete (exactly once)</p>
 * 
 * <p>The checkpoint barrier IDs are strictly monotonous increasing.</p>
 */
public class CheckpointBarrier extends RuntimeEvent {

	private long id;
	private long timestamp;

	public CheckpointBarrier() {}

	public CheckpointBarrier(long id, long timestamp) {
		this.id = id;
		this.timestamp = timestamp;
	}

	public long getId() {
		return id;
	}

	public long getTimestamp() {
		return timestamp;
	}

	// ------------------------------------------------------------------------
	
	@Override
	public void write(DataOutputView out) throws IOException {
		out.writeLong(id);
		out.writeLong(timestamp);
	}

	@Override
	public void read(DataInputView in) throws IOException {
		id = in.readLong();
		timestamp = in.readLong();
	}
	
	// ------------------------------------------------------------------------

	@Override
	public int hashCode() {
		return (int) (id ^ (id >>> 32) ^ timestamp ^(timestamp >>> 32));
	}

	@Override
	public boolean equals(Object other) {
		if (other == null || !(other instanceof CheckpointBarrier)) {
			return false;
		}
		else {
			CheckpointBarrier that = (CheckpointBarrier) other;
			return that.id == this.id && that.timestamp == this.timestamp;
		}
	}

	@Override
	public String toString() {
		return String.format("CheckpointBarrier %d @ %d", id, timestamp);
	}
}
