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

package org.apache.flink.runtime.state;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.runtime.checkpoint.metadata.ChannelStateTestUtils.randomInputChannelStateHandlesFromSameSubtask;
import static org.apache.flink.runtime.checkpoint.metadata.ChannelStateTestUtils.randomResultSubpartitionStateHandlesFromSameSubtask;
import static org.junit.jupiter.api.Assertions.assertEquals;

class MergedChannelStateHandleTest {

    @Test
    void testMergeInputChannelStateHandles() {
        List<InputChannelStateHandle> inputHandles =
                randomInputChannelStateHandlesFromSameSubtask();
        MergedInputChannelStateHandle mergedHandle =
                MergedInputChannelStateHandle.fromChannelHandles(inputHandles);
        assertEquals(inputHandles, new ArrayList<>(mergedHandle.getUnmergedHandles()));
    }

    @Test
    void testMergeResultSubpartitionStateHandles() {
        List<ResultSubpartitionStateHandle> outputHandles =
                randomResultSubpartitionStateHandlesFromSameSubtask();
        MergedResultSubpartitionStateHandle mergedHandle =
                MergedResultSubpartitionStateHandle.fromChannelHandles(outputHandles);
        assertEquals(outputHandles, new ArrayList<>(mergedHandle.getUnmergedHandles()));
    }
}
