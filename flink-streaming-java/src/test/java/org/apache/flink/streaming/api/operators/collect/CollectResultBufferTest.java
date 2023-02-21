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

package org.apache.flink.streaming.api.operators.collect;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/** Tests for {@link AbstractCollectResultBuffer} and its subclasses. */
public class CollectResultBufferTest {

    private static final TypeSerializer<Integer> serializer = IntSerializer.INSTANCE;

    @Test
    public void testUncheckpointedValidResponse() throws Exception {
        String version = "version";
        AbstractCollectResultBuffer<Integer> buffer =
                new UncheckpointedCollectResultBuffer<>(serializer, false);

        // first response to sync version, no data
        CollectCoordinationResponse response =
                new CollectCoordinationResponse(version, 0, Collections.emptyList());
        buffer.dealWithResponse(response, 0);

        List<Integer> expected = Arrays.asList(1, 2, 3);
        response = new CollectCoordinationResponse(version, 0, createSerializedResults(expected));
        buffer.dealWithResponse(response, 0);
        // for uncheckpointed buffer, results can be instantly seen by user
        for (Integer expectedValue : expected) {
            Assert.assertEquals(expectedValue, buffer.next());
        }

        expected = Arrays.asList(4, 5);
        // 3 is a retransmitted value, it should be skipped
        response =
                new CollectCoordinationResponse(
                        version, 0, createSerializedResults(Arrays.asList(3, 4, 5)));
        buffer.dealWithResponse(response, 2);
        for (Integer expectedValue : expected) {
            Assert.assertEquals(expectedValue, buffer.next());
        }
        Assert.assertNull(buffer.next());
    }

    @Test
    public void testUncheckpointedFaultTolerance() throws Exception {
        String version = "version";
        AbstractCollectResultBuffer<Integer> buffer =
                new UncheckpointedCollectResultBuffer<>(serializer, true);

        // first response to sync version, no data
        CollectCoordinationResponse response =
                new CollectCoordinationResponse(version, 0, Collections.emptyList());
        buffer.dealWithResponse(response, 0);

        List<Integer> expected = Arrays.asList(1, 2, 3);
        response = new CollectCoordinationResponse(version, 0, createSerializedResults(expected));
        buffer.dealWithResponse(response, 0);
        for (Integer expectedValue : expected) {
            Assert.assertEquals(expectedValue, buffer.next());
        }

        // version changed, job restarted
        version = "another";
        response = new CollectCoordinationResponse(version, 0, Collections.emptyList());
        buffer.dealWithResponse(response, 0);

        // retransmit same data
        response = new CollectCoordinationResponse(version, 0, createSerializedResults(expected));
        buffer.dealWithResponse(response, 0);
        for (Integer expectedValue : expected) {
            Assert.assertEquals(expectedValue, buffer.next());
        }
    }

    @Test(expected = RuntimeException.class)
    public void testUncheckpointedNotFaultTolerance() throws Exception {
        String version = "version";
        AbstractCollectResultBuffer<Integer> buffer =
                new UncheckpointedCollectResultBuffer<>(serializer, false);

        // first response to sync version, no data
        CollectCoordinationResponse response =
                new CollectCoordinationResponse(version, 0, Collections.emptyList());
        buffer.dealWithResponse(response, 0);

        List<Integer> expected = Arrays.asList(1, 2, 3);
        response = new CollectCoordinationResponse(version, 0, createSerializedResults(expected));
        buffer.dealWithResponse(response, 0);
        for (Integer expectedValue : expected) {
            Assert.assertEquals(expectedValue, buffer.next());
        }

        // version changed, job restarted
        version = "another";
        response = new CollectCoordinationResponse(version, 0, Collections.emptyList());
        buffer.dealWithResponse(response, 0);
    }

    @Test
    public void testCheckpointedValidResponse() throws Exception {
        String version = "version";
        AbstractCollectResultBuffer<Integer> buffer =
                new CheckpointedCollectResultBuffer<>(serializer);

        // first response to sync version, no data
        CollectCoordinationResponse response =
                new CollectCoordinationResponse(version, 0, Collections.emptyList());
        buffer.dealWithResponse(response, 0);

        List<Integer> expected = Arrays.asList(1, 2, 3);
        response = new CollectCoordinationResponse(version, 0, createSerializedResults(expected));
        buffer.dealWithResponse(response, 0);
        // for checkpointed buffer, results can only be seen after a checkpoint
        Assert.assertNull(buffer.next());

        response =
                new CollectCoordinationResponse(
                        version, 3, createSerializedResults(Arrays.asList(4, 5, 6)));
        buffer.dealWithResponse(response, 3);
        // results before checkpoint can be seen now
        for (Integer expectedValue : expected) {
            Assert.assertEquals(expectedValue, buffer.next());
        }

        expected = Arrays.asList(4, 5, 6);
        // 6 is a retransmitted value, it should be skipped
        response =
                new CollectCoordinationResponse(
                        version, 6, createSerializedResults(Arrays.asList(6, 7)));
        buffer.dealWithResponse(response, 5);
        // results before checkpoint can be seen now
        for (Integer expectedValue : expected) {
            Assert.assertEquals(expectedValue, buffer.next());
        }

        // send some uncommitted data
        response =
                new CollectCoordinationResponse(
                        version, 6, createSerializedResults(Arrays.asList(8, 9, 10)));
        buffer.dealWithResponse(response, 7);
        // send some committed data, but less than uncommitted data we've already sent
        expected = Arrays.asList(7);
        response =
                new CollectCoordinationResponse(
                        version, 7, createSerializedResults(Arrays.asList(8, 9)));
        buffer.dealWithResponse(response, 7);
        // results before checkpoint can be seen now
        for (Integer expectedValue : expected) {
            Assert.assertEquals(expectedValue, buffer.next());
        }

        buffer.complete();
        expected = Arrays.asList(8, 9, 10);
        for (Integer expectedValue : expected) {
            Assert.assertEquals(expectedValue, buffer.next());
        }
        Assert.assertNull(buffer.next());
    }

    @Test
    public void testCheckpointedRestart() throws Exception {
        String version = "version";
        AbstractCollectResultBuffer<Integer> buffer =
                new CheckpointedCollectResultBuffer<>(serializer);

        // first response to sync version, no data
        CollectCoordinationResponse response =
                new CollectCoordinationResponse(version, 0, Collections.emptyList());
        buffer.dealWithResponse(response, 0);

        response =
                new CollectCoordinationResponse(
                        version, 0, createSerializedResults(Arrays.asList(1, 2, 3)));
        buffer.dealWithResponse(response, 0);
        // for checkpointed buffer, results can only be seen after a checkpoint
        Assert.assertNull(buffer.next());

        // version changed, job restarted
        version = "another";
        response = new CollectCoordinationResponse(version, 0, Collections.emptyList());
        buffer.dealWithResponse(response, 0);

        List<Integer> expected = Arrays.asList(4, 5, 6);
        // transmit some different data, they should overwrite previous ones
        response = new CollectCoordinationResponse(version, 0, createSerializedResults(expected));
        buffer.dealWithResponse(response, 0);
        // checkpoint still not done
        Assert.assertNull(buffer.next());

        // checkpoint completed
        response = new CollectCoordinationResponse(version, 3, Collections.emptyList());
        buffer.dealWithResponse(response, 0);
        for (Integer expectedValue : expected) {
            Assert.assertEquals(expectedValue, buffer.next());
        }
        Assert.assertNull(buffer.next());
    }

    @Test
    public void testImmediateAccumulatorResult() throws Exception {
        String version = "version";
        AbstractCollectResultBuffer<Integer> buffer =
                new UncheckpointedCollectResultBuffer<>(serializer, false);

        // job finished before the first request,
        // so the first and only response is from the accumulator and contains results
        List<Integer> expected = Arrays.asList(1, 2, 3);
        CollectCoordinationResponse response =
                new CollectCoordinationResponse(version, 0, createSerializedResults(expected));
        buffer.dealWithResponse(response, 0);
        buffer.complete();

        for (Integer expectedValue : expected) {
            Assert.assertEquals(expectedValue, buffer.next());
        }
        Assert.assertNull(buffer.next());
    }

    private List<byte[]> createSerializedResults(List<Integer> values) throws Exception {
        List<byte[]> serializedResults = new ArrayList<>();
        for (int value : values) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputView wrapper = new DataOutputViewStreamWrapper(baos);
            serializer.serialize(value, wrapper);
            serializedResults.add(baos.toByteArray());
        }
        return serializedResults;
    }
}
