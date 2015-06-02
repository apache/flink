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

package org.apache.flink.runtime.jobgraph.tasks;

/**
 * This interface must be implemented by invokable operators (subclasses
 * of {@link org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable} that
 * participate in state checkpoints.
 */
public interface CheckpointedOperator {

	/**
	 * This method is either called directly and asynchronously by the checkpoint
	 * coordinator (in the case of functions that are directly notified - usually
	 * the data sources), or called synchronously when all incoming channels have
	 * reported a checkpoint barrier.
	 * 
	 * @param checkpointId The ID of the checkpoint, incrementing.
	 * @param timestamp The timestamp when the checkpoint was triggered at the JobManager.
	 */
	void triggerCheckpoint(long checkpointId, long timestamp) throws Exception;
}
