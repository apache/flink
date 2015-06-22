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

import java.io.IOException;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.event.task.TaskEvent;

/**
 * Checkpoint barriers are used to synchronize checkpoints throughout the streaming topology. The
 * barriers are emitted by the sources when instructed to do so by the JobManager. When
 * operators receive a {@link CheckpointBarrier} on one of its inputs it must block processing
 * of further elements on this input until all inputs received the checkpoint barrier
 * corresponding to to that checkpoint. Once all inputs received the checkpoint barrier for
 * a checkpoint the operator is to perform the checkpoint and then broadcast the barrier to
 * downstream operators.
 *
 * <p>
 * The checkpoint barrier IDs are advancing. Once an operator receives a {@link CheckpointBarrier}
 * for a checkpoint with a higher id it is to discard all barriers that it received from previous
 * checkpoints and unblock all other inputs.
 */
public class CheckpointBarrier extends TaskEvent {

	protected long id;
	protected long timestamp;

	public CheckpointBarrier() {}

	public CheckpointBarrier(long id, long timestamp) {
		this.id = id;
		this.timestamp = timestamp;
	}

	public long getId() {
		return id;
	}

	public long getTimestamp() {
		return id;
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
