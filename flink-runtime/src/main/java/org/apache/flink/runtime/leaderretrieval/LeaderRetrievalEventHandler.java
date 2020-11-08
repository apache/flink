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

package org.apache.flink.runtime.leaderretrieval;

import org.apache.flink.runtime.leaderelection.LeaderInformation;

/**
 * Interface which should be implemented to notify to {@link LeaderInformation} changes in
 * {@link LeaderRetrievalDriver}.
 *
 * <p><strong>Important</strong>: The {@link LeaderRetrievalDriver} could not guarantee that there is no
 * {@link LeaderRetrievalEventHandler} callbacks happen after {@link LeaderRetrievalDriver#close()}. This
 * means that the implementor of {@link LeaderRetrievalEventHandler} is responsible for filtering out
 * spurious callbacks(e.g. after close has been called on {@link LeaderRetrievalDriver}).
 */
public interface LeaderRetrievalEventHandler {

	/**
	 * Called by specific {@link LeaderRetrievalDriver} to notify leader address.
	 *
	 * <p>Duplicated leader change events could happen, so the implementation should check whether
	 * the passed leader information is truly changed with last stored leader information.
	 *
	 * @param leaderInformation the new leader information to notify {@link LeaderRetrievalService}. It could be
	 * {@link LeaderInformation#empty()} if the leader address does not exist in the external storage.
	 */
	void notifyLeaderAddress(LeaderInformation leaderInformation);
}
