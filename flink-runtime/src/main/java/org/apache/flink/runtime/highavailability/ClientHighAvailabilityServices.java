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

package org.apache.flink.runtime.highavailability;

import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;

/**
 * {@code ClientHighAvailabilityServices} provides services those are required
 * on client-side. At the moment only the REST endpoint leader retriever is required
 * because all communication between the client and cluster happens via the REST endpoint.
 */
public interface ClientHighAvailabilityServices extends AutoCloseable {

	/**
	 * Get the leader retriever for the cluster's rest endpoint.
	 *
	 * @return the leader retriever for cluster's rest endpoint.
	 */
	LeaderRetrievalService getClusterRestEndpointLeaderRetriever();

}
