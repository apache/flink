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

/**
 * Savepoint store used to persist {@link Savepoint} instances.
 *
 * <p>The main implementation is the {@link FsSavepointStore}. We also have the
 * {@link HeapSavepointStore} for historical reasons (introduced in Flink 1.0).
 */
public interface SavepointStore {

	/**
	 * Stores the savepoint.
	 *
	 * @param savepoint Savepoint to be stored
	 * @param <T>       Savepoint type
	 * @return Path of stored savepoint
	 * @throws Exception Failures during store are forwarded
	 */
	<T extends Savepoint> String storeSavepoint(T savepoint) throws Exception;

	/**
	 * Loads the savepoint at the specified path.
	 *
	 * @param path Path of savepoint to load
	 * @return The loaded savepoint
	 * @throws Exception Failures during load are forwared
	 */
	Savepoint loadSavepoint(String path) throws Exception;

	/**
	 * Disposes the savepoint at the specified path.
	 *
	 * <p>The class loader is needed, because savepoints can currently point to
	 * arbitrary snapshot {@link org.apache.flink.runtime.state.StateHandle}
	 * instances, which need the user code class loader for deserialization.
	 *
	 * @param path        Path of savepoint to dispose
	 * @param classLoader Class loader for disposal
	 * @throws Exception Failures during diposal are forwarded
	 */
	void disposeSavepoint(String path, ClassLoader classLoader) throws Exception;

	/**
	 * Shut downs the savepoint store.
	 *
	 * <p>Only necessary for implementations where the savepoint life-cycle is
	 * bound to the cluster life-cycle.
	 *
	 * @throws Exception Failures during shut down are forwarded
	 */
	void shutdown() throws Exception;

}
