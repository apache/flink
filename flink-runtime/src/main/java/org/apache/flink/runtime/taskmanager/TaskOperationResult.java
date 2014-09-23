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

package org.apache.flink.runtime.taskmanager;

import java.io.IOException;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.util.StringUtils;

import com.google.common.base.Preconditions;


public class TaskOperationResult implements IOReadableWritable, java.io.Serializable {
	
	private static final long serialVersionUID = -3852292420229699888L;

	
	private ExecutionAttemptID executionId;
	
	private boolean success;
	
	private String description;


	public TaskOperationResult() {
		this(new ExecutionAttemptID(), false);
	}
	
	public TaskOperationResult(ExecutionAttemptID executionId, boolean success) {
		this(executionId, success, null);
	}
	
	public TaskOperationResult(ExecutionAttemptID executionId, boolean success, String description) {
		Preconditions.checkNotNull(executionId);
		
		this.executionId = executionId;
		this.success = success;
		this.description = description;
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
	
	// --------------------------------------------------------------------------------------------
	//  Serialization
	// --------------------------------------------------------------------------------------------

	@Override
	public void read(DataInputView in) throws IOException {
		this.executionId.read(in);
		this.success = in.readBoolean();
		this.description = StringUtils.readNullableString(in);
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		this.executionId.write(out);
		out.writeBoolean(success);
		StringUtils.writeNullableString(description, out);
	}
	
	// --------------------------------------------------------------------------------------------
	//  Utilities
	// --------------------------------------------------------------------------------------------
	
	@Override
	public String toString() {
		return String.format("TaskOperationResult %s [%s]%s", executionId, 
				success ? "SUCCESS" : "FAILED", description == null ? "" : " - " + description);
	}
}
