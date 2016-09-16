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

package org.apache.flink.runtime.checkpoint;

/**
 * A checkpoint ID counter.
 */
public interface CheckpointIDCounter {

	/**
	 * Starts the {@link CheckpointIDCounter} service.
	 */
	void start() throws Exception;

	/**
	 * Shuts the {@link CheckpointIDCounter} service down and frees all created
	 * resources.
	 */
	void shutdown() throws Exception;

	/**
	 * Suspends the counter.
	 *
	 * <p>If the implementation allows recovery, the counter state needs to be
	 * kept. Otherwise, this acts as shutdown.
	 */
	void suspend() throws Exception;

	/**
	 * Atomically increments the current checkpoint ID.
	 *
	 * @return The previous checkpoint ID
	 */
	long getAndIncrement() throws Exception;

	/**
	 * Sets the current checkpoint ID.
	 *
	 * @param newId The new ID
	 */
	void setCount(long newId) throws Exception;

}
