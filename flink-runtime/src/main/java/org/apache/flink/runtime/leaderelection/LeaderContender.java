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

import java.util.UUID;

/**
 * Interface which has to be implemented to take part in the leader election process of the {@link
 * LeaderElection}.
 *
 * <p>The event handling methods for granting and revoking leadership are meant to be called in an
 * alternating fashion, i.e. no two leadership grants or revocation events can happen after each
 * other. Two subsequent events of the same type would indicate that the current instance missed a
 * state change of the HA backend resulting in an invalid state of the overall deployment
 * (split-brain scenario possible).
 */
public interface LeaderContender {

    /**
     * Callback method which is called by the {@link LeaderElectionService} upon selecting this
     * instance as the new leader. The method is called with the new leader session ID.
     *
     * @param leaderSessionID New leader session ID
     */
    void grantLeadership(UUID leaderSessionID);

    /**
     * Callback method which is called by the {@link LeaderElectionService} upon revoking the
     * leadership of a former leader. This might happen in case that multiple contenders have been
     * granted leadership.
     */
    void revokeLeadership();

    /**
     * Callback method which is called by {@link LeaderElectionService} in case of an error in the
     * service thread.
     *
     * @param exception Caught exception
     */
    void handleError(Exception exception);
}
