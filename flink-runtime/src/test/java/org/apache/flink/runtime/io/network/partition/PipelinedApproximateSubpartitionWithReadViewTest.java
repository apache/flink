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

package org.apache.flink.runtime.io.network.partition;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertTrue;

/**
 * Additional tests for {@link PipelinedApproximateSubpartitionView} which require an availability
 * listener and a read view.
 *
 * @see PipelinedSubpartitionTest
 */
public class PipelinedApproximateSubpartitionWithReadViewTest
        extends PipelinedSubpartitionWithReadViewTest {

    @Before
    @Override
    public void before() throws IOException {
        setup(ResultPartitionType.PIPELINED_APPROXIMATE);
        subpartition = new PipelinedApproximateSubpartition(0, 2, resultPartition);
        availablityListener = new AwaitableBufferAvailablityListener();
        readView = subpartition.createReadView(availablityListener);
    }

    @Test
    @Override
    public void testRelease() {
        readView.releaseAllResources();
        assertTrue(
                resultPartition
                        .getPartitionManager()
                        .getUnreleasedPartitions()
                        .contains(resultPartition.getPartitionId()));
    }
}
