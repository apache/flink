/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.operators.lifecycle.event;

import org.apache.flink.streaming.api.operators.StreamOperator;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * An event of calling {@link StreamOperator#finish() finish}. Serves two purposes:
 *
 * <ol>
 *   <li>Verify that the call actually happened
 *   <li>Collect sent/receive info to verify that there was no data loss
 * </ol>
 */
public class OperatorFinishedEvent extends TestEvent {
    final LastReceivedVertexDataInfo receiveInfo;
    public final long lastSent;

    public OperatorFinishedEvent(
            String operatorId,
            int subtaskIndex,
            int attemptNumber,
            long lastSent,
            LastReceivedVertexDataInfo receiveInfo) {
        super(operatorId, subtaskIndex, attemptNumber);
        this.receiveInfo = receiveInfo;
        this.lastSent = lastSent;
    }

    public long getLastReceived(String upstreamID, int upstreamIndex) {
        return receiveInfo.forUpstream(upstreamID).getOrDefault(upstreamIndex, -1L);
    }

    @Override
    public String toString() {
        return String.format("%s; receive=%s; sent=%d", super.toString(), receiveInfo, lastSent);
    }

    /** LastVertexDataInfo. */
    public static class LastVertexDataInfo implements Serializable {
        public final Map<Integer, Long> bySubtask = new HashMap<>();

        @Override
        public String toString() {
            return String.valueOf(bySubtask);
        }
    }

    /** LastReceivedVertexDataInfo. */
    public static class LastReceivedVertexDataInfo implements Serializable {
        private final Map<String, LastVertexDataInfo> byUpstreamOperatorID;

        public LastReceivedVertexDataInfo(Map<String, LastVertexDataInfo> byUpstreamOperatorID) {
            this.byUpstreamOperatorID = byUpstreamOperatorID;
        }

        public Map<Integer, Long> forUpstream(String upstreamID) {
            return byUpstreamOperatorID.getOrDefault(upstreamID, new LastVertexDataInfo())
                    .bySubtask;
        }

        @Override
        public String toString() {
            return String.valueOf(byUpstreamOperatorID);
        }
    }
}
