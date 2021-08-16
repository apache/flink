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

package org.apache.flink.connector.kafka.sink;

import org.junit.Test;

import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Tests for {@link TransactionalIdFactory}. */
public class TransactionIdFactoryTest {

    @Test
    public void testBuildTransactionalId() {
        final String expected = "prefix-0-2";
        assertEquals(expected, TransactionalIdFactory.buildTransactionalId("prefix", 0, 2L));
    }

    @Test
    public void testParseStateFromIdWithNotEnoughDelimiters() {
        final String transactionalId = "prefix-0";
        assertFalse(TransactionalIdFactory.parseKafkaWriterState(transactionalId).isPresent());
    }

    @Test
    public void testParseStateFromId() {
        final String transactionalId = "prefix-0-2";
        final Optional<KafkaWriterState> stateOpt =
                TransactionalIdFactory.parseKafkaWriterState(transactionalId);
        assertTrue(stateOpt.isPresent());
        assertEquals(new KafkaWriterState("prefix", 0, 2L), stateOpt.get());
    }

    @Test
    public void testParseStateFromIdWithPrefixContainingDelimiter() {
        final String transactionalId = "prefix1-prefix2-0-2";
        final Optional<KafkaWriterState> stateOpt =
                TransactionalIdFactory.parseKafkaWriterState(transactionalId);
        assertTrue(stateOpt.isPresent());
        assertEquals(new KafkaWriterState("prefix1-prefix-2", 0, 2L), stateOpt.get());
    }

    @Test
    public void testParseStateFromIdWithInvalidSubtaskId() {
        final String transactionalId = "prefix-invalid-2";
        assertFalse(TransactionalIdFactory.parseKafkaWriterState(transactionalId).isPresent());
    }

    @Test
    public void testParseStateFromIdWithInvalidCheckpointOffset() {
        final String transactionalId = "prefix-0-invalid";
        assertFalse(TransactionalIdFactory.parseKafkaWriterState(transactionalId).isPresent());
    }
}
