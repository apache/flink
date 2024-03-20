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

import org.apache.flink.util.function.ThrowingRunnable;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * {@code LeaderElection} serves as a proxy between {@code LeaderElectionService} and {@link
 * LeaderContender}.
 */
public interface LeaderElection extends AutoCloseable {

    /** Registers the passed {@link LeaderContender} with the leader election process. */
    void startLeaderElection(LeaderContender contender) throws Exception;

    /**
     * Confirms that the {@link LeaderContender} has accepted the leadership identified by the given
     * leader session id. It also publishes the leader address under which the leader is reachable.
     *
     * <p>The intention of this method is to establish an order between setting the new leader
     * session ID in the {@link LeaderContender} and publishing the new leader session ID and the
     * related leader address to the leader retrieval services.
     *
     * <p>Calling this method does not have any effect if the leadership was lost in the meantime.
     *
     * @param leaderSessionID The new leader session ID
     * @param leaderAddress The address of the new leader
     */
    void confirmLeadership(UUID leaderSessionID, String leaderAddress);

    /**
     * Runs the passed {@code callback} if the leadership is still acquired when executing the
     * {@code callback}.
     *
     * @param leaderSessionID The leadership session this {@code callback} is assigned to.
     * @param callback The callback that shall be executed.
     * @param eventLabelToLog A label that's used for logging the event handling.
     * @return The future that can be used to monitor the completion of the asynchronous operation.
     *     The future should complete exceptionally with a {@link LeadershipLostException} if the
     *     callback could not be triggered due to missing leadership.
     */
    CompletableFuture<Void> runAsyncIfLeader(
            UUID leaderSessionID,
            ThrowingRunnable<? extends Throwable> callback,
            String eventLabelToLog);

    /**
     * Closes the {@code LeaderElection} by deregistering the {@link LeaderContender} from the
     * underlying leader election. {@link LeaderContender#revokeLeadership()} will be called if the
     * service still holds the leadership.
     */
    void close() throws Exception;
}
