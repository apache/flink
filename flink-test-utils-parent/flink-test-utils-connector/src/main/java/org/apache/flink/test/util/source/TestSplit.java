/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.test.util.source;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.core.io.SimpleVersionedSerializer;

/**
 * A simple test split implementation that can be reused across test sources. This provides a
 * minimal split that is sufficient for most testing scenarios.
 */
@PublicEvolving
public class TestSplit implements SourceSplit {

    /** Singleton instance for reuse across all test sources. */
    public static final TestSplit INSTANCE = new TestSplit();

    /** Serializer for TestSplit instances. */
    public static final SimpleVersionedSerializer<TestSplit> SERIALIZER = new TestSplitSerializer();

    private static final String SPLIT_ID = "test-split";

    private TestSplit() {
        // Private constructor for singleton pattern
    }

    @Override
    public String splitId() {
        return SPLIT_ID;
    }

    @Override
    public String toString() {
        return "TestSplit{id=" + SPLIT_ID + "}";
    }

    /** Serializer for TestSplit that always returns the singleton instance. */
    private static class TestSplitSerializer implements SimpleVersionedSerializer<TestSplit> {

        @Override
        public int getVersion() {
            return 1;
        }

        @Override
        public byte[] serialize(TestSplit split) {
            return new byte[0];
        }

        @Override
        public TestSplit deserialize(int version, byte[] serialized) {
            return INSTANCE;
        }
    }
}
