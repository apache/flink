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

public class StreamingSuperstep extends TaskEvent {

	protected long id;
	protected long timestamp;

	public StreamingSuperstep() {}

	public StreamingSuperstep(long id, long timestamp) {
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
		if (other == null || !(other instanceof StreamingSuperstep)) {
			return false;
		}
		else {
			StreamingSuperstep that = (StreamingSuperstep) other;
			return that.id == this.id && that.timestamp == this.timestamp;
		}
	}

	@Override
	public String toString() {
		return String.format("StreamingSuperstep %d @ %d", id, timestamp);
	}
}
