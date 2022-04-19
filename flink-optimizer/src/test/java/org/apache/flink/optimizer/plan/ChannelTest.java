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

package org.apache.flink.optimizer.plan;

import org.apache.flink.api.common.operators.GenericDataSourceBase;
import org.apache.flink.api.common.operators.OperatorInformation;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.optimizer.dag.DataSourceNode;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ChannelTest {

    @Test
    void testGetEstimatesNoReplicationFactor() {
        final long NUM_RECORD = 1001;
        final long SIZE = 467131;

        DataSourceNode source = getSourceNode();
        SourcePlanNode planNode = new SourcePlanNode(source, "test node");
        Channel channel = new Channel(planNode);

        // no estimates here
        assertThat(channel.getEstimatedOutputSize()).isEqualTo(-1);
        assertThat(channel.getEstimatedNumRecords()).isEqualTo(-1);

        // set estimates
        source.setEstimatedNumRecords(NUM_RECORD);
        source.setEstimatedOutputSize(SIZE);
        assertThat(channel.getEstimatedOutputSize()).isEqualTo(SIZE);
        assertThat(channel.getEstimatedNumRecords()).isEqualTo(NUM_RECORD);
    }

    @Test
    void testGetEstimatesWithReplicationFactor() {
        final long NUM_RECORD = 1001;
        final long SIZE = 467131;

        final int REPLICATION = 23;

        DataSourceNode source = getSourceNode();
        SourcePlanNode planNode = new SourcePlanNode(source, "test node");
        Channel channel = new Channel(planNode);
        channel.setReplicationFactor(REPLICATION);

        // no estimates here
        assertThat(channel.getEstimatedOutputSize()).isEqualTo(-1);
        assertThat(channel.getEstimatedNumRecords()).isEqualTo(-1);

        // set estimates
        source.setEstimatedNumRecords(NUM_RECORD);
        source.setEstimatedOutputSize(SIZE);
        assertThat(channel.getEstimatedOutputSize()).isEqualTo(SIZE * REPLICATION);
        assertThat(channel.getEstimatedNumRecords()).isEqualTo(NUM_RECORD * REPLICATION);
    }

    //	private static final OptimizerNode getSingleInputNode() {
    //		return new MapNode(new MapOperatorBase<String, String, GenericMap<String,String>>(
    //				new IdentityMapper<String>(),
    //				new UnaryOperatorInformation<String, String>(BasicTypeInfo.STRING_TYPE_INFO,
    // BasicTypeInfo.STRING_TYPE_INFO),
    //				"map"));
    //	}

    private static final DataSourceNode getSourceNode() {
        return new DataSourceNode(
                new GenericDataSourceBase<String, TextInputFormat>(
                        new TextInputFormat(new Path("/ignored")),
                        new OperatorInformation<String>(BasicTypeInfo.STRING_TYPE_INFO),
                        "source"));
    }
}
