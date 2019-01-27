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

package org.apache.flink.runtime.io.network.partition.external;

import org.apache.flink.runtime.io.network.partition.ResultPartitionID;

import java.io.IOException;
import java.util.Set;

/**
 * LocalResultPartitionResolver is responsible for resolving where a result partition locates
 * by {@link ResultPartitionID}. Since LocalResultPartitionResolver directly deals with
 * specific storage, it may also need to do recycling in most user scenarios.
 */
public abstract class LocalResultPartitionResolver {
	protected ExternalBlockShuffleServiceConfiguration shuffleServiceConfiguration;

	LocalResultPartitionResolver(ExternalBlockShuffleServiceConfiguration shuffleServiceConfiguration) {
		this.shuffleServiceConfiguration = shuffleServiceConfiguration;
	}

	/**
	 * Gets the exact directory of the result partition.
	 *
	 * @param resultPartitionID The id of the result partition.
	 * @return Tuple2.f0 is the configured root directory at which the result partition locates,
	 * 		Tuple2.f1 is the full directory path of the result partition.
	 */
	abstract ResultPartitionFileInfo getResultPartitionDir(ResultPartitionID resultPartitionID) throws IOException;

	/**
	 * Recycles result partition's directory and its meta information.
	 *
	 * @param resultPartitionID
	 */
	abstract void recycleResultPartition(ResultPartitionID resultPartitionID);

	/**
	 * Notifies an application's initialization.
	 *
	 * @param user User name of the application.
	 * @param appId Id of the application.
	 */
	abstract void initializeApplication(String user, String appId);

	/**
	 * Notifies an application is stopped, so it's meta information can be safely deleted.
	 *
	 * @param appId Id of the application.
	 * @return Result partitions of this application.
	 */
	abstract Set<ResultPartitionID> stopApplication(String appId);

	/**
	 * Notifies LocalResultPartitionResolver to stop elegantly.
	 */
	abstract void stop();

	/**
	 * The information for the files of the external result partition.
	 */
	interface ResultPartitionFileInfo {
		/**
		 * The root directory of the result partition.
		 *
		 * @return the root directory.
		 */
		String getRootDir();

		/**
		 * The result partition's directory path.
		 *
		 * @return the result partition's directory path.
		 */
		String getPartitionDir();

		/**
		 * Get the time interval in milliseconds to delete this result partition after they are fully consumed.
		 *
		 * @return the time interval to delete this result partition after they are fully consumed.
		 */
		long getConsumedPartitionTTL();

		/**
		 * Get the time interval in milliseconds to delete this partition since the last access when it has not been fully consumed.
		 *
		 * @return the time interval to delete this partition since the last access when it has not been fully consumed.
		 */
		long getPartialConsumedPartitionTTL();
	}
}
