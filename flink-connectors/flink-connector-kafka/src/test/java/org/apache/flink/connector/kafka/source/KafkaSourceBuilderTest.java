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

package org.apache.flink.connector.kafka.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.TestLogger;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Test;

/** Tests for {@link KafkaSourceBuilder}. */
public class KafkaSourceBuilderTest extends TestLogger {

    @Test
    public void testBuildSourceWithoutGroupId() {
        new KafkaSourceBuilder<String>()
                .setBootstrapServers("testServer")
                .setTopics("topic")
                .setDeserializer(
                        new KafkaRecordDeserializer<String>() {
                            @Override
                            public TypeInformation<String> getProducedType() {
                                return null;
                            }

                            @Override
                            public void deserialize(
                                    ConsumerRecord<byte[], byte[]> record,
                                    Collector<String> collector)
                                    throws Exception {}
                        })
                .build();
    }
}
