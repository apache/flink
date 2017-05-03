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

package org.apache.flink.streaming.api.checkpoint;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.runtime.checkpoint.MasterTriggerRestoreHook;

/**
 * This interface can be implemented by streaming functions that need to trigger a
 * "global action" on the master (in the checkpoint coordinator) as part of every
 * checkpoint and restore operation.
 *
 * @param <E> The type of the data stored by the hook in the checkpoint, or {@code Void}, if none.
 */
@PublicEvolving
public interface WithMasterCheckpointHook<E> extends java.io.Serializable {

	/**
	 * Creates the hook that should be called by the checkpoint coordinator.
	 */
	MasterTriggerRestoreHook<E> createMasterTriggerRestoreHook();
}
