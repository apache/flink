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

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link EdgeDistributionPatternSnapshot}. */
class EdgeDistributionPatternSnapshotTest {

    @Test
    void roundTripSerialization() throws IOException {
        Map<OperatorID, DistributionPattern[]> patterns = new HashMap<>();
        OperatorID op1 = new OperatorID();
        OperatorID op2 = new OperatorID();
        patterns.put(op1, new DistributionPattern[] {DistributionPattern.POINTWISE});
        patterns.put(
                op2,
                new DistributionPattern[] {
                    DistributionPattern.ALL_TO_ALL, DistributionPattern.POINTWISE
                });

        EdgeDistributionPatternSnapshot original = new EdgeDistributionPatternSnapshot(patterns);
        byte[] bytes = original.toBytes();
        EdgeDistributionPatternSnapshot restored = EdgeDistributionPatternSnapshot.fromBytes(bytes);

        assertThat(restored.getOutputPatterns(op1)).containsExactly(DistributionPattern.POINTWISE);
        assertThat(restored.getOutputPatterns(op2))
                .containsExactly(DistributionPattern.ALL_TO_ALL, DistributionPattern.POINTWISE);
    }

    @Test
    void emptySnapshot() throws IOException {
        EdgeDistributionPatternSnapshot original =
                new EdgeDistributionPatternSnapshot(new HashMap<>());
        byte[] bytes = original.toBytes();
        EdgeDistributionPatternSnapshot restored = EdgeDistributionPatternSnapshot.fromBytes(bytes);

        assertThat(restored.getOutputPatterns(new OperatorID())).isNull();
    }

    @Test
    void getOutputPatternByIndex() throws IOException {
        Map<OperatorID, DistributionPattern[]> patterns = new HashMap<>();
        OperatorID op = new OperatorID();
        patterns.put(
                op,
                new DistributionPattern[] {
                    DistributionPattern.POINTWISE,
                    DistributionPattern.ALL_TO_ALL,
                    DistributionPattern.POINTWISE
                });

        EdgeDistributionPatternSnapshot snapshot = new EdgeDistributionPatternSnapshot(patterns);

        assertThat(snapshot.getOutputPattern(op, 0)).isEqualTo(DistributionPattern.POINTWISE);
        assertThat(snapshot.getOutputPattern(op, 1)).isEqualTo(DistributionPattern.ALL_TO_ALL);
        assertThat(snapshot.getOutputPattern(op, 2)).isEqualTo(DistributionPattern.POINTWISE);
        assertThat(snapshot.getOutputPattern(op, 3)).isNull();
        assertThat(snapshot.getOutputPattern(new OperatorID(), 0)).isNull();
    }

    @Test
    void unknownPatternByteRejected() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        dos.writeInt(1); // version
        dos.writeInt(1); // numOperators
        dos.writeLong(0L); // opId lower
        dos.writeLong(0L); // opId upper
        dos.writeInt(1); // numPartitions
        dos.writeByte(7); // unknown pattern byte
        dos.flush();

        assertThatThrownBy(() -> EdgeDistributionPatternSnapshot.fromBytes(baos.toByteArray()))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Unknown DistributionPattern byte");
    }
}
