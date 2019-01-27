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

package org.apache.flink.runtime.jobmaster.failover;

import javax.annotation.Nonnull;

/**
 * A store for recording the {@link OperationLog} of the {@link org.apache.flink.runtime.jobmaster.JobMaster}.
 */
interface OperationLogStore {
	/**
	 * Start the store, usually should reset the reader and writer.
	 */
	void start();

	/**
	 * Stop the store, usually should close the reader and writer.
	 */
	void stop();

	/**
	 * Clear all the logs in the store.
	 */
	void clear();

	/**
	 * Write an operation log.
	 *
	 * @param opLog The operation log need to be record
	 */
	void writeOpLog(@Nonnull OperationLog opLog);

	/**
	 * Return a iterator of operation logs in the store.
	 *
	 * @return a iterator of operation logs in the store
	 */
	OperationLog readOpLog();
}
