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

package org.apache.flink.runtime.rest.messages.checkpoints;

import org.apache.flink.runtime.rest.messages.JobVertexIdPathParameter;
import org.apache.flink.runtime.rest.messages.MessagePathParameter;

import java.util.Arrays;
import java.util.Collection;

/**
 * Message parameters for subtask related checkpoint message.
 *
 * <p>The message requires a JobID, checkpoint ID and a JobVertexID to be specified.
 */
public class TaskCheckpointMessageParameters extends CheckpointMessageParameters {

	protected final JobVertexIdPathParameter jobVertexIdPathParameter = new JobVertexIdPathParameter();

	@Override
	public Collection<MessagePathParameter<?>> getPathParameters() {
		return Arrays.asList(jobPathParameter, checkpointIdPathParameter, jobVertexIdPathParameter);
	}
}
