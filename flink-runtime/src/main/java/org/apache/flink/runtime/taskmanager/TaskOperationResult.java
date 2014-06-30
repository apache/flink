/**
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

package org.apache.flink.runtime.taskmanager;

import java.io.IOException;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.types.StringValue;

import com.google.common.base.Preconditions;


public class TaskOperationResult implements IOReadableWritable {

	private JobVertexID vertexId;
	
	private int subtaskIndex;
	
	private ExecutionAttemptID executionId;
	
	private boolean success;
	
	private String description;


	public TaskOperationResult() {
		this(new JobVertexID(), -1, new ExecutionAttemptID(), false);
	}
	
	public TaskOperationResult(JobVertexID vertexId, int subtaskIndex, ExecutionAttemptID executionId, boolean success) {
		this(vertexId, subtaskIndex, executionId, success, null);
	}
	
	public TaskOperationResult(JobVertexID vertexId, int subtaskIndex, ExecutionAttemptID executionId, boolean success, String description) {
		Preconditions.checkNotNull(vertexId);
		Preconditions.checkNotNull(executionId);
		
		this.vertexId = vertexId;
		this.subtaskIndex = subtaskIndex;
		this.executionId = executionId;
		this.success = success;
		this.description = description;
	}


	public JobVertexID getVertexId() {
		return vertexId;
	}
	
	public int getSubtaskIndex() {
		return subtaskIndex;
	}
	
	public ExecutionAttemptID getExecutionId() {
		return executionId;
	}
	
	public boolean isSuccess() {
		return success;
	}

	public String getDescription() {
		return description;
	}

	@Override
	public void read(DataInputView in) throws IOException {
		this.vertexId.read(in);
		this.subtaskIndex = in.readInt();
		this.success = in.readBoolean();
		
		if (in.readBoolean()) {
			this.description = StringValue.readString(in);
		}
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		this.vertexId.write(out);
		out.writeInt(subtaskIndex);
		out.writeBoolean(success);
		
		if (description != null) {
			out.writeBoolean(true);
			StringValue.writeString(description, out);
		} else {
			out.writeBoolean(false);
		}
	}
}
