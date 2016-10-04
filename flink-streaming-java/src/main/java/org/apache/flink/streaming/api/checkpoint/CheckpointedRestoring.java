/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.checkpoint;

import org.apache.flink.annotation.PublicEvolving;

import java.io.Serializable;

/**
 * This deprecated interface contains the methods for restoring from the legacy checkpointing mechanism of state.
 * @param <T> type of the restored state.
 */
@Deprecated
@PublicEvolving
public interface CheckpointedRestoring<T extends Serializable> {
	/**
	 * Restores the state of the function or operator to that of a previous checkpoint.
	 * This method is invoked when a function is executed as part of a recovery run.
	 *
	 * Note that restoreState() is called before open().
	 *
	 * @param state The state to be restored.
	 */
	void restoreState(T state) throws Exception;
}
