/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.leaderelection;

import java.util.UUID;

/** Leader election event. */
public abstract class LeaderElectionEvent {
    public boolean isIsLeaderEvent() {
        return false;
    }

    public boolean isNotLeaderEvent() {
        return false;
    }

    public boolean isLeaderInformationChangeEvent() {
        return false;
    }

    public boolean isAllKnownLeaderInformationEvent() {
        return false;
    }

    public boolean isErrorEvent() {
        return false;
    }

    public IsLeaderEvent asIsLeaderEvent() {
        return as(IsLeaderEvent.class);
    }

    public <T> T as(Class<T> clazz) {
        if (clazz.isAssignableFrom(getClass())) {
            return clazz.cast(this);
        } else {
            throw new IllegalStateException("Cannot cast object.");
        }
    }

    public static class IsLeaderEvent extends LeaderElectionEvent {

        private final UUID leaderSessionID;

        public IsLeaderEvent(UUID leaderSessionID) {
            this.leaderSessionID = leaderSessionID;
        }

        public UUID getLeaderSessionID() {
            return leaderSessionID;
        }

        @Override
        public boolean isIsLeaderEvent() {
            return true;
        }
    }

    public static class NotLeaderEvent extends LeaderElectionEvent {
        @Override
        public boolean isNotLeaderEvent() {
            return true;
        }
    }

    public static class LeaderInformationChangeEvent extends LeaderElectionEvent {
        private final String componentId;
        private final LeaderInformation leaderInformation;

        LeaderInformationChangeEvent(String componentId, LeaderInformation leaderInformation) {
            this.componentId = componentId;
            this.leaderInformation = leaderInformation;
        }

        public LeaderInformation getLeaderInformation() {
            return leaderInformation;
        }

        public String getComponentId() {
            return componentId;
        }

        @Override
        public boolean isLeaderInformationChangeEvent() {
            return true;
        }
    }

    public static class AllLeaderInformationChangeEvent extends LeaderElectionEvent {
        private final LeaderInformationRegister leaderInformationRegister;

        AllLeaderInformationChangeEvent(LeaderInformationRegister leaderInformationRegister) {
            this.leaderInformationRegister = leaderInformationRegister;
        }

        @Override
        public boolean isAllKnownLeaderInformationEvent() {
            return true;
        }

        public LeaderInformationRegister getLeaderInformationRegister() {
            return leaderInformationRegister;
        }
    }

    /**
     * A {@code LeaderElectionEvent} that's triggered by {@link
     * LeaderElectionDriver.Listener#onError(Throwable)}.
     */
    public static class ErrorEvent extends LeaderElectionEvent {

        private final Throwable error;

        ErrorEvent(Throwable error) {
            this.error = error;
        }

        public Throwable getError() {
            return error;
        }

        @Override
        public boolean isErrorEvent() {
            return true;
        }
    }
}
