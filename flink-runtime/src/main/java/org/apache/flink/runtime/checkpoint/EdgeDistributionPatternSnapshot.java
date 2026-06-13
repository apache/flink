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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.OperatorID;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Snapshot of per-operator output edge {@link DistributionPattern}s, persisted via {@link
 * MasterTriggerRestoreHook} / {@link MasterState} so that the correct old distribution pattern is
 * available when restoring from a checkpoint after a shuffle-mode change.
 *
 * <p>Key = outputOperatorID (generated, chain-tail), Value = one {@link DistributionPattern} per
 * output partition (indexed by partitionIndex).
 */
public class EdgeDistributionPatternSnapshot {

    public static final String HOOK_IDENTIFIER = "__flink_edge_distribution_patterns__";

    private static final int FORMAT_VERSION = 1;
    private static final byte PATTERN_POINTWISE = 0;
    private static final byte PATTERN_ALL_TO_ALL = 1;

    private final Map<OperatorID, DistributionPattern[]> outputEdgePatterns;

    public EdgeDistributionPatternSnapshot(
            Map<OperatorID, DistributionPattern[]> outputEdgePatterns) {
        this.outputEdgePatterns = Collections.unmodifiableMap(new HashMap<>(outputEdgePatterns));
    }

    @Nullable
    public DistributionPattern[] getOutputPatterns(OperatorID outputOperatorID) {
        DistributionPattern[] patterns = outputEdgePatterns.get(outputOperatorID);
        return patterns != null ? patterns.clone() : null;
    }

    @Nullable
    public DistributionPattern getOutputPattern(OperatorID outputOperatorID, int partitionIndex) {
        DistributionPattern[] patterns = outputEdgePatterns.get(outputOperatorID);
        if (patterns == null || partitionIndex < 0 || partitionIndex >= patterns.length) {
            return null;
        }
        return patterns[partitionIndex];
    }

    public byte[] toBytes() throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(baos)) {
            dos.writeInt(FORMAT_VERSION);
            dos.writeInt(outputEdgePatterns.size());
            for (Map.Entry<OperatorID, DistributionPattern[]> entry :
                    outputEdgePatterns.entrySet()) {
                OperatorID opId = entry.getKey();
                dos.writeLong(opId.getLowerPart());
                dos.writeLong(opId.getUpperPart());
                DistributionPattern[] patterns = entry.getValue();
                dos.writeInt(patterns.length);
                for (DistributionPattern p : patterns) {
                    dos.writeByte(
                            p == DistributionPattern.ALL_TO_ALL
                                    ? PATTERN_ALL_TO_ALL
                                    : PATTERN_POINTWISE);
                }
            }
            dos.flush();
            return baos.toByteArray();
        }
    }

    public static EdgeDistributionPatternSnapshot fromBytes(byte[] bytes) throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
                DataInputStream dis = new DataInputStream(bais)) {
            int version = dis.readInt();
            if (version != FORMAT_VERSION) {
                throw new IOException(
                        "Unsupported EdgeDistributionPatternSnapshot version: " + version);
            }
            int numOperators = dis.readInt();
            Map<OperatorID, DistributionPattern[]> map = new HashMap<>(numOperators);
            for (int i = 0; i < numOperators; i++) {
                OperatorID opId = new OperatorID(dis.readLong(), dis.readLong());
                int numPartitions = dis.readInt();
                DistributionPattern[] patterns = new DistributionPattern[numPartitions];
                for (int j = 0; j < numPartitions; j++) {
                    byte b = dis.readByte();
                    if (b == PATTERN_ALL_TO_ALL) {
                        patterns[j] = DistributionPattern.ALL_TO_ALL;
                    } else if (b == PATTERN_POINTWISE) {
                        patterns[j] = DistributionPattern.POINTWISE;
                    } else {
                        throw new IOException("Unknown DistributionPattern byte in snapshot: " + b);
                    }
                }
                map.put(opId, patterns);
            }
            return new EdgeDistributionPatternSnapshot(map);
        }
    }
}
