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

import org.junit.Assert;
import org.junit.Test;

public class ChannelTest {

    @Test
    public void testGetEstimatesNoReplicationFactor() {
        final long NUM_RECORD = 1001;
        final long SIZE = 467131;

        DataSourceNode source = getSourceNode();
        SourcePlanNode planNode = new SourcePlanNode(source, "test node");
        Channel channel = new Channel(planNode);

        // no estimates here
        Assert.assertEquals(-1, channel.getEstimatedOutputSize());
        Assert.assertEquals(-1, channel.getEstimatedNumRecords());

        // set estimates
        source.setEstimatedNumRecords(NUM_RECORD);
        source.setEstimatedOutputSize(SIZE);
        Assert.assertEquals(SIZE, channel.getEstimatedOutputSize());
        Assert.assertEquals(NUM_RECORD, channel.getEstimatedNumRecords());
    }

    @Test
    public void testGetEstimatesWithReplicationFactor() {
        final long NUM_RECORD = 1001;
        final long SIZE = 467131;

        final int REPLICATION = 23;

        DataSourceNode source = getSourceNode();
        SourcePlanNode planNode = new SourcePlanNode(source, "test node");
        Channel channel = new Channel(planNode);
        channel.setReplicationFactor(REPLICATION);

        // no estimates here
        Assert.assertEquals(-1, channel.getEstimatedOutputSize());
        Assert.assertEquals(-1, channel.getEstimatedNumRecords());

        // set estimates
        source.setEstimatedNumRecords(NUM_RECORD);
        source.setEstimatedOutputSize(SIZE);
        Assert.assertEquals(SIZE * REPLICATION, channel.getEstimatedOutputSize());
        Assert.assertEquals(NUM_RECORD * REPLICATION, channel.getEstimatedNumRecords());
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
