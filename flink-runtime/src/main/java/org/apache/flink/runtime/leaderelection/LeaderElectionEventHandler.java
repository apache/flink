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

/**
 * Interface which should be implemented to respond to leader changes in {@link LeaderElectionDriver}.
 *
 * <p><strong>Important</strong>: The {@link LeaderElectionDriver} could not guarantee that there is no
 * {@link LeaderElectionEventHandler} callbacks happen after {@link LeaderElectionDriver#close()}. This
 * means that the implementor of {@link LeaderElectionEventHandler} is responsible for filtering out
 * spurious callbacks(e.g. after close has been called on {@link LeaderElectionDriver}).
 */
public interface LeaderElectionEventHandler {

	/**
	 * Called by specific {@link LeaderElectionDriver} when the leadership is granted.
	 */
	void onGrantLeadership();

	/**
	 * Called by specific {@link LeaderElectionDriver} when the leadership is revoked.
	 */
	void onRevokeLeadership();

	/**
	 * Called by specific {@link LeaderElectionDriver} when the leader information is changed. Then the
	 * {@link LeaderElectionService} could write the leader information again if necessary. This method
	 * should only be called when {@link LeaderElectionDriver#hasLeadership()} is true. Duplicated leader
	 * change events could happen, so the implementation should check whether the passed leader information
	 * is really different with internal confirmed leader information.
	 *
	 * @param leaderInformation leader information which contains leader session id and leader address.
	 */
	void onLeaderInformationChange(LeaderInformation leaderInformation);
}
