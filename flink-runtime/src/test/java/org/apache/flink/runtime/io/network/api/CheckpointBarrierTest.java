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

package org.apache.flink.runtime.io.network.api;

import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;

import org.junit.Test;

import static org.junit.Assert.fail;

/** Tests for the {@link CheckpointBarrier} type. */
public class CheckpointBarrierTest {

    /**
     * Test serialization of the checkpoint barrier. The checkpoint barrier does not support its own
     * serialization, in order to be immutable.
     */
    @Test
    public void testSerialization() throws Exception {
        long id = Integer.MAX_VALUE + 123123L;
        long timestamp = Integer.MAX_VALUE + 1228L;

        CheckpointOptions options = CheckpointOptions.forCheckpointWithDefaultLocation();
        CheckpointBarrier barrier = new CheckpointBarrier(id, timestamp, options);

        try {
            barrier.write(new DataOutputSerializer(1024));
            fail("should throw an exception");
        } catch (UnsupportedOperationException e) {
            // expected
        }

        try {
            barrier.read(new DataInputDeserializer(new byte[32]));
            fail("should throw an exception");
        } catch (UnsupportedOperationException e) {
            // expected
        }
    }
}
