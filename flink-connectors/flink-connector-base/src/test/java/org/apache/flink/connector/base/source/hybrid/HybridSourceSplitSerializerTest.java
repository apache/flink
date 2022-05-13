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

package org.apache.flink.connector.base.source.hybrid;

import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.mocks.MockSource;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/** Tests for {@link HybridSourceSplitSerializer}. */
public class HybridSourceSplitSerializerTest {

    @Test
    public void testSerialization() throws Exception {
        Map<Integer, Source> switchedSources = new HashMap<>();
        switchedSources.put(0, new MockSource(null, 0));
        byte[] splitBytes = {1, 2, 3};
        HybridSourceSplitSerializer serializer = new HybridSourceSplitSerializer();
        HybridSourceSplit split = new HybridSourceSplit(0, splitBytes, 0, "splitId");
        byte[] serialized = serializer.serialize(split);
        HybridSourceSplit clonedSplit = serializer.deserialize(0, serialized);
        Assert.assertEquals(split, clonedSplit);

        try {
            serializer.deserialize(1, serialized);
            Assert.fail();
        } catch (IOException e) {
            // expected invalid version
        }
    }
}
