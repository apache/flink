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

package org.apache.flink.runtime.checkpoint.savepoint;

import java.util.Collection;
import org.apache.flink.runtime.checkpoint.TaskState;

/**
 * A savepoint factory returning savepoints of a specific versioned
 * subtype.
 *
 * <p>I'm wondering how useful this is in practice in comparison to simply
 * returning the base Savepoint type.
 *
 * @param <T> Concrete versioned savepoint subtype
 */
interface SavepointFactory<T extends Savepoint> {

	/**
	 * Create a special savepoint of subtype T.
	 *
	 * @param checkpointId Checkpoint ID of the savepoint.
	 * @param taskStates Task states of the savepoint.
	 * @return A concrete savepoint subtype of type T.
	 */
	T createSavepoint(long checkpointId, Collection<TaskState> taskStates);

}
