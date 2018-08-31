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

package org.apache.flink.runtime.state;

import org.apache.flink.annotation.Internal;

import javax.annotation.Nullable;

/**
 * Interface for operators that can perform snapshots of their state.
 *
 * @param <S> Generic type of the state object that is created as handle to snapshots.
 * @param <R> Generic type of the state object that used in restore.
 */
@Internal
public interface Snapshotable<S extends StateObject, R> extends SnapshotStrategy<S> {

	/**
	 * Restores state that was previously snapshotted from the provided parameters. Typically the parameters are state
	 * handles from which the old state is read.
	 *
	 * @param state the old state to restore.
	 */
	void restore(@Nullable R state) throws Exception;
}
