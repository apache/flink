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

package org.apache.flink.optimizer.dataproperties;

import org.apache.flink.api.common.operators.SemanticProperties;
import org.apache.flink.api.common.operators.SingleInputSemanticProperties;
import org.apache.flink.api.common.operators.util.FieldSet;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class GlobalPropertiesPushdownTest {

    @Test
    void testAnyPartitioningPushedDown() {
        try {
            RequestedGlobalProperties req = new RequestedGlobalProperties();
            req.setAnyPartitioning(new FieldSet(3, 1));

            RequestedGlobalProperties preserved =
                    req.filterBySemanticProperties(getAllPreservingSemProps(), 0);
            assertThat(preserved.getPartitioning())
                    .isEqualTo(PartitioningProperty.ANY_PARTITIONING);
            assertThat(preserved.getPartitionedFields().isValidSubset(new FieldSet(1, 3))).isTrue();

            RequestedGlobalProperties nonPreserved =
                    req.filterBySemanticProperties(getNonePreservingSemProps(), 0);
            assertThat(nonPreserved == null || nonPreserved.isTrivial()).isTrue();
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    void testHashPartitioningPushedDown() {
        try {
            RequestedGlobalProperties req = new RequestedGlobalProperties();
            req.setHashPartitioned(new FieldSet(3, 1));

            RequestedGlobalProperties preserved =
                    req.filterBySemanticProperties(getAllPreservingSemProps(), 0);
            assertThat(preserved.getPartitioning())
                    .isEqualTo(PartitioningProperty.HASH_PARTITIONED);
            assertThat(preserved.getPartitionedFields().isValidSubset(new FieldSet(1, 3))).isTrue();

            RequestedGlobalProperties nonPreserved =
                    req.filterBySemanticProperties(getNonePreservingSemProps(), 0);
            assertThat(nonPreserved == null || nonPreserved.isTrivial()).isTrue();
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    void testCustomPartitioningNotPushedDown() {
        try {
            RequestedGlobalProperties req = new RequestedGlobalProperties();
            req.setCustomPartitioned(new FieldSet(3, 1), new MockPartitioner());

            RequestedGlobalProperties pushedDown =
                    req.filterBySemanticProperties(getAllPreservingSemProps(), 0);
            assertThat(pushedDown == null || pushedDown.isTrivial()).isTrue();
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    void testForcedReblancingNotPushedDown() {
        try {
            RequestedGlobalProperties req = new RequestedGlobalProperties();
            req.setForceRebalancing();

            RequestedGlobalProperties pushedDown =
                    req.filterBySemanticProperties(getAllPreservingSemProps(), 0);
            assertThat(pushedDown == null || pushedDown.isTrivial()).isTrue();
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    // --------------------------------------------------------------------------------------------

    private static SemanticProperties getAllPreservingSemProps() {
        return new SingleInputSemanticProperties.AllFieldsForwardedProperties();
    }

    private static SemanticProperties getNonePreservingSemProps() {
        return new SingleInputSemanticProperties();
    }
}
