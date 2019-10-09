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

package org.apache.flink.runtime.leaderelection;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Optional;

/**
 * Interface for a store which allows mutually exclusive access by the leader.
 */
public interface LeaderStore {

	byte[] EMPTY_DATA = new byte[0];

	/**
	 * Returns true if the given path exists in the leader store.
	 *
	 * @param path The path whose existence is retrieved.
	 * @return True if the given path exists in the leader store.
	 */
	boolean exists(@Nonnull String path) throws Exception;

	/**
	 * Returns the data associated with the given path in the leader store.
	 *
	 * @param path The path whose associated data is retrieved.
	 * @return The data associated with the given path.
	 */
	Optional<byte[]> get(@Nonnull String path) throws Exception;

	/**
	 * Returns the children under the given path in the leader store.
	 *
	 * @param path The path whose children are retrieved.
	 * @return The children under the give path.
	 */
	Optional<Collection<String>> getChildren(@Nonnull String path) throws Exception;

	/**
	 * Creates the given path with the data associated in the leader store.
	 *
	 * @param path The path with which the given data is associated.
	 * @param data The data to be associated with the given path.
	 */
	void add(@Nonnull String path, @Nullable byte[] data) throws Exception;

	/**
	 * Removes the given path in the leader store.
	 *
	 * @param path The path to be removed.
	 */
	void remove(@Nonnull String path) throws Exception;

	/**
	 * Updates the data of the given path in the leader store.
	 *
	 * @param path The path whose data to be updated.
	 * @param data The data to update.
	 */
	void update(@Nonnull String path, @Nullable byte[] data) throws Exception;

}
