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

package org.apache.flink.streaming.api.functions.source.datagen;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;

import java.io.Serializable;
import java.util.Iterator;

/**
 * Stateful and re-scalable data generator.
 */
@Experimental
public interface DataGenerator<T> extends Serializable, Iterator<T> {

	/**
	 * Open and initialize state for {@link DataGenerator}.
	 * See {@link CheckpointedFunction#initializeState}.
	 *
	 * @param name The state of {@link DataGenerator} should related to this name, make sure
	 *             the name of state is different.
	 */
	void open(
			String name,
			FunctionInitializationContext context,
			RuntimeContext runtimeContext) throws Exception;

	/**
	 * Snapshot state for {@link DataGenerator}.
	 * See {@link CheckpointedFunction#snapshotState}.
	 */
	default void snapshotState(FunctionSnapshotContext context) throws Exception {}
}
