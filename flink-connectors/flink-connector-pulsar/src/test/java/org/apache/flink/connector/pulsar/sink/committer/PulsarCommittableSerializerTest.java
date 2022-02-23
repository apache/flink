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

package org.apache.flink.connector.pulsar.sink.committer;

import org.apache.pulsar.client.api.transaction.TxnID;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for serializing and deserializing {@link PulsarCommittable} with {@link
 * PulsarCommittableSerializer}.
 */
class PulsarCommittableSerializerTest {

    private static final PulsarCommittableSerializer INSTANCE = new PulsarCommittableSerializer();

    @Test
    void committableSerDe() throws IOException {
        String topic = randomAlphabetic(10);
        TxnID txnID =
                new TxnID(
                        ThreadLocalRandom.current().nextLong(),
                        ThreadLocalRandom.current().nextLong());

        PulsarCommittable committable = new PulsarCommittable(txnID, topic);

        byte[] bytes = INSTANCE.serialize(committable);
        PulsarCommittable committable1 = INSTANCE.deserialize(INSTANCE.getVersion(), bytes);

        assertEquals(committable1, committable);
    }
}
