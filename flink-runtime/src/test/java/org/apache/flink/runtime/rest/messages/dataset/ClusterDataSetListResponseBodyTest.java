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

package org.apache.flink.runtime.rest.messages.dataset;

import org.apache.flink.runtime.io.network.partition.DataSetMetaInfo;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.rest.messages.RestResponseMarshallingTestBase;
import org.apache.flink.testutils.junit.extensions.parameterized.NoOpTestExtension;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.StringUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ClusterDataSetListResponseBody}. */
@ExtendWith(NoOpTestExtension.class)
class ClusterDataSetListResponseBodyTest
        extends RestResponseMarshallingTestBase<ClusterDataSetListResponseBody> {

    @Test
    void testFrom() {
        final Map<IntermediateDataSetID, DataSetMetaInfo> originalDataSets = new HashMap<>();
        originalDataSets.put(
                new IntermediateDataSetID(), DataSetMetaInfo.withNumRegisteredPartitions(1, 2));
        originalDataSets.put(
                new IntermediateDataSetID(), DataSetMetaInfo.withNumRegisteredPartitions(2, 2));

        List<ClusterDataSetEntry> convertedDataSets =
                ClusterDataSetListResponseBody.from(originalDataSets).getDataSets();
        assertThat(convertedDataSets).hasSize(2);
        for (ClusterDataSetEntry convertedDataSet : convertedDataSets) {
            IntermediateDataSetID id =
                    new IntermediateDataSetID(
                            new AbstractID(
                                    StringUtils.hexStringToByte(convertedDataSet.getDataSetId())));

            DataSetMetaInfo dataSetMetaInfo = originalDataSets.get(id);

            assertThat(convertedDataSet.isComplete())
                    .isEqualTo(
                            dataSetMetaInfo.getNumRegisteredPartitions().orElse(0)
                                    == dataSetMetaInfo.getNumTotalPartitions());
        }
    }

    @Override
    protected Class<ClusterDataSetListResponseBody> getTestResponseClass() {
        return ClusterDataSetListResponseBody.class;
    }

    @Override
    protected ClusterDataSetListResponseBody getTestResponseInstance() throws Exception {
        final Map<IntermediateDataSetID, DataSetMetaInfo> dataSets = new HashMap<>();
        dataSets.put(
                new IntermediateDataSetID(), DataSetMetaInfo.withNumRegisteredPartitions(1, 2));
        return ClusterDataSetListResponseBody.from(dataSets);
    }

    @Override
    protected void assertOriginalEqualsToUnmarshalled(
            ClusterDataSetListResponseBody expected, ClusterDataSetListResponseBody actual) {
        final List<ClusterDataSetEntry> expectedDataSets = expected.getDataSets();
        final List<ClusterDataSetEntry> actualDataSets = actual.getDataSets();

        assertThat(actualDataSets).hasSize(expectedDataSets.size());
        for (int i = 0; i < expectedDataSets.size(); i++) {
            ClusterDataSetEntry expectedDataSet = expectedDataSets.get(i);
            ClusterDataSetEntry actualDataSet = actualDataSets.get(i);

            assertThat(actualDataSet.getDataSetId()).isEqualTo(expectedDataSet.getDataSetId());
            assertThat(actualDataSet.isComplete()).isEqualTo(expectedDataSet.isComplete());
        }
    }
}
