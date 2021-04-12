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

package org.apache.flink.runtime.io.network.partition.consumer;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.io.network.partition.NoOpResultSubpartitionView;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGateTest.TestingResultPartitionManager;

import org.hamcrest.Matchers;
import org.junit.Test;

import static org.apache.flink.runtime.io.network.partition.consumer.SingleInputGateTest.verifyBufferOrEvent;
import static org.apache.flink.runtime.util.NettyShuffleDescriptorBuilder.createRemoteWithIdAndLocation;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/** Tests for {@link UnionInputGate}. */
public class UnionInputGateTest extends InputGateTestBase {

    /**
     * Tests basic correctness of buffer-or-event interleaving and correct <code>null</code> return
     * value after receiving all end-of-partition events.
     *
     * <p>For buffer-or-event instances, it is important to verify that they have been set off to
     * the correct logical index.
     */
    @Test(timeout = 120 * 1000)
    public void testBasicGetNextLogic() throws Exception {
        // Setup
        final SingleInputGate ig1 = createInputGate(3);
        final SingleInputGate ig2 = createInputGate(5);

        final UnionInputGate union = new UnionInputGate(new SingleInputGate[] {ig1, ig2});

        assertEquals(
                ig1.getNumberOfInputChannels() + ig2.getNumberOfInputChannels(),
                union.getNumberOfInputChannels());

        final TestInputChannel[][] inputChannels =
                new TestInputChannel[][] {
                    TestInputChannel.createInputChannels(ig1, 3),
                    TestInputChannel.createInputChannels(ig2, 5)
                };

        inputChannels[0][0].readBuffer(); // 0 => 0
        inputChannels[0][0].readEndOfPartitionEvent(); // 0 => 0
        inputChannels[1][2].readBuffer(); // 2 => 5
        inputChannels[1][2].readEndOfPartitionEvent(); // 2 => 5
        inputChannels[1][0].readBuffer(); // 0 => 3
        inputChannels[1][1].readBuffer(); // 1 => 4
        inputChannels[0][1].readBuffer(); // 1 => 1
        inputChannels[1][3].readBuffer(); // 3 => 6
        inputChannels[0][1].readEndOfPartitionEvent(); // 1 => 1
        inputChannels[1][3].readEndOfPartitionEvent(); // 3 => 6
        inputChannels[0][2].readBuffer(); // 1 => 2
        inputChannels[0][2].readEndOfPartitionEvent(); // 1 => 2
        inputChannels[1][4].readBuffer(); // 4 => 7
        inputChannels[1][4].readEndOfPartitionEvent(); // 4 => 7
        inputChannels[1][1].readEndOfPartitionEvent(); // 0 => 3
        inputChannels[1][0].readEndOfPartitionEvent(); // 0 => 3

        ig1.notifyChannelNonEmpty(inputChannels[0][0]);
        ig1.notifyChannelNonEmpty(inputChannels[0][1]);
        ig1.notifyChannelNonEmpty(inputChannels[0][2]);

        ig2.notifyChannelNonEmpty(inputChannels[1][0]);
        ig2.notifyChannelNonEmpty(inputChannels[1][1]);
        ig2.notifyChannelNonEmpty(inputChannels[1][2]);
        ig2.notifyChannelNonEmpty(inputChannels[1][3]);
        ig2.notifyChannelNonEmpty(inputChannels[1][4]);

        verifyBufferOrEvent(union, true, 0, true); // gate 1, channel 0
        verifyBufferOrEvent(union, true, 3, true); // gate 2, channel 0
        verifyBufferOrEvent(union, true, 1, true); // gate 1, channel 1
        verifyBufferOrEvent(union, true, 4, true); // gate 2, channel 1
        verifyBufferOrEvent(union, true, 2, true); // gate 1, channel 2
        verifyBufferOrEvent(union, true, 5, true); // gate 2, channel 1
        verifyBufferOrEvent(union, false, 0, true); // gate 1, channel 0
        verifyBufferOrEvent(union, true, 6, true); // gate 2, channel 1
        verifyBufferOrEvent(union, false, 1, true); // gate 1, channel 1
        verifyBufferOrEvent(union, true, 7, true); // gate 2, channel 1
        verifyBufferOrEvent(union, false, 2, true); // gate 1, channel 2
        verifyBufferOrEvent(union, false, 3, true); // gate 2, channel 0
        verifyBufferOrEvent(union, false, 4, true); // gate 2, channel 1
        verifyBufferOrEvent(union, false, 5, true); // gate 2, channel 2
        verifyBufferOrEvent(union, false, 6, true); // gate 2, channel 3
        verifyBufferOrEvent(union, false, 7, false); // gate 2, channel 4

        // Return null when the input gate has received all end-of-partition events
        assertTrue(union.isFinished());
        assertFalse(union.getNext().isPresent());
    }

    @Test
    public void testIsAvailable() throws Exception {
        final SingleInputGate inputGate1 = createInputGate(1);
        TestInputChannel inputChannel1 = new TestInputChannel(inputGate1, 0);
        inputGate1.setInputChannels(inputChannel1);

        final SingleInputGate inputGate2 = createInputGate(1);
        TestInputChannel inputChannel2 = new TestInputChannel(inputGate2, 0);
        inputGate2.setInputChannels(inputChannel2);

        testIsAvailable(new UnionInputGate(inputGate1, inputGate2), inputGate1, inputChannel1);
    }

    @Test
    public void testIsAvailableAfterFinished() throws Exception {
        final SingleInputGate inputGate1 = createInputGate(1);
        TestInputChannel inputChannel1 = new TestInputChannel(inputGate1, 0);
        inputGate1.setInputChannels(inputChannel1);

        final SingleInputGate inputGate2 = createInputGate(1);
        TestInputChannel inputChannel2 = new TestInputChannel(inputGate2, 0);
        inputGate2.setInputChannels(inputChannel2);

        testIsAvailableAfterFinished(
                new UnionInputGate(inputGate1, inputGate2),
                () -> {
                    inputChannel1.readEndOfPartitionEvent();
                    inputChannel2.readEndOfPartitionEvent();
                    inputGate1.notifyChannelNonEmpty(inputChannel1);
                    inputGate2.notifyChannelNonEmpty(inputChannel2);
                });
    }

    @Test
    public void testUpdateInputChannel() throws Exception {
        final SingleInputGate inputGate1 = createInputGate(1);
        TestInputChannel inputChannel1 = new TestInputChannel(inputGate1, 0);
        inputGate1.setInputChannels(inputChannel1);

        final SingleInputGate inputGate2 = createInputGate(1);
        TestingResultPartitionManager partitionManager =
                new TestingResultPartitionManager(new NoOpResultSubpartitionView());
        InputChannel unknownInputChannel2 =
                InputChannelBuilder.newBuilder()
                        .setPartitionManager(partitionManager)
                        .buildUnknownChannel(inputGate2);
        inputGate2.setInputChannels(unknownInputChannel2);

        UnionInputGate unionInputGate = new UnionInputGate(inputGate1, inputGate2);
        ResultPartitionID resultPartitionID = unknownInputChannel2.getPartitionId();
        ResourceID location = ResourceID.generate();
        inputGate2.updateInputChannel(
                location,
                createRemoteWithIdAndLocation(resultPartitionID.getPartitionId(), location));

        assertThat(unionInputGate.getChannel(0), Matchers.is(inputChannel1));
        // Check that updated input channel is visible via UnionInputGate
        assertThat(unionInputGate.getChannel(1), Matchers.is(inputGate2.getChannel(0)));
    }

    @Test
    public void testGetChannelWithShiftedGateIndexes() {
        gateIndex = 2;
        final SingleInputGate inputGate1 = createInputGate(1);
        TestInputChannel inputChannel1 = new TestInputChannel(inputGate1, 0);
        inputGate1.setInputChannels(inputChannel1);

        final SingleInputGate inputGate2 = createInputGate(1);
        TestInputChannel inputChannel2 = new TestInputChannel(inputGate2, 0);
        inputGate2.setInputChannels(inputChannel2);

        UnionInputGate unionInputGate = new UnionInputGate(inputGate1, inputGate2);

        assertThat(unionInputGate.getChannel(0), Matchers.is(inputChannel1));
        // Check that updated input channel is visible via UnionInputGate
        assertThat(unionInputGate.getChannel(1), Matchers.is(inputChannel2));
    }
}
