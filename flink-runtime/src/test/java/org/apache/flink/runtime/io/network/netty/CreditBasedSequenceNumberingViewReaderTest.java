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

package org.apache.flink.runtime.io.network.netty;

import org.apache.flink.runtime.io.network.partition.NoOpResultSubpartitionView;
import org.apache.flink.runtime.io.network.partition.TestingResultPartition;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;

import org.apache.flink.shaded.netty4.io.netty.channel.embedded.EmbeddedChannel;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Tests for {@link CreditBasedSequenceNumberingViewReader}. */
public class CreditBasedSequenceNumberingViewReaderTest {

    @Test
    public void testResumeConsumption() throws Exception {
        int numCredits = 2;
        CreditBasedSequenceNumberingViewReader reader1 =
                createNetworkSequenceViewReader(numCredits);

        reader1.resumeConsumption();
        assertEquals(numCredits, reader1.getNumCreditsAvailable());

        reader1.addCredit(numCredits);
        reader1.resumeConsumption();
        assertEquals(2 * numCredits, reader1.getNumCreditsAvailable());

        CreditBasedSequenceNumberingViewReader reader2 = createNetworkSequenceViewReader(0);

        reader2.addCredit(numCredits);
        assertEquals(numCredits, reader2.getNumCreditsAvailable());

        reader2.resumeConsumption();
        assertEquals(0, reader2.getNumCreditsAvailable());
    }

    @Test
    public void testNeedAnnounceBacklog() throws Exception {
        int numCredits = 2;
        CreditBasedSequenceNumberingViewReader reader1 =
                createNetworkSequenceViewReader(numCredits);

        assertFalse(reader1.needAnnounceBacklog());
        reader1.addCredit(-numCredits);
        assertFalse(reader1.needAnnounceBacklog());

        CreditBasedSequenceNumberingViewReader reader2 = createNetworkSequenceViewReader(0);
        assertTrue(reader2.needAnnounceBacklog());

        reader2.addCredit(numCredits);
        assertFalse(reader2.needAnnounceBacklog());

        reader2.addCredit(-numCredits);
        assertTrue(reader2.needAnnounceBacklog());
    }

    private CreditBasedSequenceNumberingViewReader createNetworkSequenceViewReader(
            int initialCredit) throws Exception {
        PartitionRequestQueue queue = new PartitionRequestQueue();
        EmbeddedChannel channel = new EmbeddedChannel(queue);
        channel.close();
        CreditBasedSequenceNumberingViewReader reader =
                new CreditBasedSequenceNumberingViewReader(
                        new InputChannelID(), initialCredit, queue);
        reader.notifySubpartitionCreated(
                TestingResultPartition.newBuilder()
                        .setCreateSubpartitionViewFunction(
                                (index, listener) -> new NoOpResultSubpartitionView())
                        .build(),
                0);
        return reader;
    }
}
