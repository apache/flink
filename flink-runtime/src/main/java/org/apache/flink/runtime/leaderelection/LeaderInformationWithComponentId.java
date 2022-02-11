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

import java.util.Objects;

/** Leader information and its corresponding leader name. */
public final class LeaderInformationWithComponentId {
    private final String componentId;

    private final LeaderInformation leaderInformation;

    private LeaderInformationWithComponentId(
            String componentId, LeaderInformation leaderInformation) {
        this.componentId = componentId;
        this.leaderInformation = leaderInformation;
    }

    LeaderInformation getLeaderInformation() {
        return leaderInformation;
    }

    String getComponentId() {
        return componentId;
    }

    public static LeaderInformationWithComponentId create(
            String componentId, LeaderInformation leaderInformation) {
        return new LeaderInformationWithComponentId(componentId, leaderInformation);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LeaderInformationWithComponentId that = (LeaderInformationWithComponentId) o;
        return Objects.equals(componentId, that.componentId)
                && Objects.equals(leaderInformation, that.leaderInformation);
    }

    @Override
    public int hashCode() {
        return Objects.hash(componentId, leaderInformation);
    }
}
