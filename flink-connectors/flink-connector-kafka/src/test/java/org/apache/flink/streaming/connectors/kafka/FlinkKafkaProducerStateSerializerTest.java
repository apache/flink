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

package org.apache.flink.streaming.connectors.kafka;

import org.apache.flink.api.common.typeutils.SerializerTestBase;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction.TransactionHolder;

import java.util.Collections;
import java.util.Optional;

/** A test for the {@link TypeSerializer TypeSerializers} used for the Kafka producer state. */
public class FlinkKafkaProducerStateSerializerTest
        extends SerializerTestBase<
                TwoPhaseCommitSinkFunction.State<
                        FlinkKafkaProducer.KafkaTransactionState,
                        FlinkKafkaProducer.KafkaTransactionContext>> {

    @Override
    protected TypeSerializer<
                    TwoPhaseCommitSinkFunction.State<
                            FlinkKafkaProducer.KafkaTransactionState,
                            FlinkKafkaProducer.KafkaTransactionContext>>
            createSerializer() {
        return new TwoPhaseCommitSinkFunction.StateSerializer<>(
                new FlinkKafkaProducer.TransactionStateSerializer(),
                new FlinkKafkaProducer.ContextStateSerializer());
    }

    @Override
    protected Class<
                    TwoPhaseCommitSinkFunction.State<
                            FlinkKafkaProducer.KafkaTransactionState,
                            FlinkKafkaProducer.KafkaTransactionContext>>
            getTypeClass() {
        return (Class) TwoPhaseCommitSinkFunction.State.class;
    }

    @Override
    protected int getLength() {
        return -1;
    }

    @Override
    protected TwoPhaseCommitSinkFunction.State<
                    FlinkKafkaProducer.KafkaTransactionState,
                    FlinkKafkaProducer.KafkaTransactionContext>
            [] getTestData() {
        //noinspection unchecked
        return new TwoPhaseCommitSinkFunction.State[] {
            new TwoPhaseCommitSinkFunction.State<
                    FlinkKafkaProducer.KafkaTransactionState,
                    FlinkKafkaProducer.KafkaTransactionContext>(
                    new TransactionHolder(
                            new FlinkKafkaProducer.KafkaTransactionState(
                                    "fake", 1L, (short) 42, null),
                            0),
                    Collections.emptyList(),
                    Optional.empty()),
            new TwoPhaseCommitSinkFunction.State<
                    FlinkKafkaProducer.KafkaTransactionState,
                    FlinkKafkaProducer.KafkaTransactionContext>(
                    new TransactionHolder(
                            new FlinkKafkaProducer.KafkaTransactionState(
                                    "fake", 1L, (short) 42, null),
                            2711),
                    Collections.singletonList(
                            new TransactionHolder(
                                    new FlinkKafkaProducer.KafkaTransactionState(
                                            "fake", 1L, (short) 42, null),
                                    42)),
                    Optional.empty()),
            new TwoPhaseCommitSinkFunction.State<
                    FlinkKafkaProducer.KafkaTransactionState,
                    FlinkKafkaProducer.KafkaTransactionContext>(
                    new TransactionHolder(
                            new FlinkKafkaProducer.KafkaTransactionState(
                                    "fake", 1L, (short) 42, null),
                            0),
                    Collections.emptyList(),
                    Optional.of(
                            new FlinkKafkaProducer.KafkaTransactionContext(
                                    Collections.emptySet()))),
            new TwoPhaseCommitSinkFunction.State<
                    FlinkKafkaProducer.KafkaTransactionState,
                    FlinkKafkaProducer.KafkaTransactionContext>(
                    new TransactionHolder(
                            new FlinkKafkaProducer.KafkaTransactionState(
                                    "fake", 1L, (short) 42, null),
                            0),
                    Collections.emptyList(),
                    Optional.of(
                            new FlinkKafkaProducer.KafkaTransactionContext(
                                    Collections.singleton("hello")))),
            new TwoPhaseCommitSinkFunction.State<
                    FlinkKafkaProducer.KafkaTransactionState,
                    FlinkKafkaProducer.KafkaTransactionContext>(
                    new TransactionHolder(
                            new FlinkKafkaProducer.KafkaTransactionState(
                                    "fake", 1L, (short) 42, null),
                            0),
                    Collections.singletonList(
                            new TransactionHolder(
                                    new FlinkKafkaProducer.KafkaTransactionState(
                                            "fake", 1L, (short) 42, null),
                                    0)),
                    Optional.of(
                            new FlinkKafkaProducer.KafkaTransactionContext(
                                    Collections.emptySet()))),
            new TwoPhaseCommitSinkFunction.State<
                    FlinkKafkaProducer.KafkaTransactionState,
                    FlinkKafkaProducer.KafkaTransactionContext>(
                    new TransactionHolder(
                            new FlinkKafkaProducer.KafkaTransactionState(
                                    "fake", 1L, (short) 42, null),
                            0),
                    Collections.singletonList(
                            new TransactionHolder(
                                    new FlinkKafkaProducer.KafkaTransactionState(
                                            "fake", 1L, (short) 42, null),
                                    0)),
                    Optional.of(
                            new FlinkKafkaProducer.KafkaTransactionContext(
                                    Collections.singleton("hello"))))
        };
    }

    @Override
    public void testInstantiate() {
        // this serializer does not support instantiation
    }
}
