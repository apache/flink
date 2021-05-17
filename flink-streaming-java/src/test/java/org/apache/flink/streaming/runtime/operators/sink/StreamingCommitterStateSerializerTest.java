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

package org.apache.flink.streaming.runtime.operators.sink;

import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;

import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

/** Test {@link StreamingCommitterStateSerializer}. */
public class StreamingCommitterStateSerializerTest {

    @Test
    public void serializeNonEmptyState() throws IOException {
        final StreamingCommitterState<String> expectedStreamingCommitterState =
                new StreamingCommitterState<>(Arrays.asList("city", "great", "temper", "valley"));
        final StreamingCommitterStateSerializer<String> streamingCommitterStateSerializer =
                new StreamingCommitterStateSerializer<>(SimpleVersionedStringSerializer.INSTANCE);

        final byte[] serialize =
                streamingCommitterStateSerializer.serialize(expectedStreamingCommitterState);
        final StreamingCommitterState<String> streamingCommitterState =
                streamingCommitterStateSerializer.deserialize(
                        streamingCommitterStateSerializer.getVersion(), serialize);

        assertThat(
                streamingCommitterState.getCommittables(),
                equalTo(expectedStreamingCommitterState.getCommittables()));
    }

    @Test
    public void serializeEmptyState() throws IOException {
        final StreamingCommitterState<String> expectedStreamingCommitterState =
                new StreamingCommitterState<>(Collections.emptyList());
        final StreamingCommitterStateSerializer<String> streamingCommitterStateSerializer =
                new StreamingCommitterStateSerializer<>(SimpleVersionedStringSerializer.INSTANCE);

        final byte[] serialize =
                streamingCommitterStateSerializer.serialize(expectedStreamingCommitterState);
        final StreamingCommitterState<String> streamingCommitterState =
                streamingCommitterStateSerializer.deserialize(
                        streamingCommitterStateSerializer.getVersion(), serialize);

        assertThat(
                streamingCommitterState.getCommittables(),
                equalTo(expectedStreamingCommitterState.getCommittables()));
    }
}
