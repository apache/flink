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

package org.example;


import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.testcontainers.shaded.org.bouncycastle.util.Strings;


public class CustomKafkaSerializer implements
        KafkaRecordSerializationSchema<DecorateRecord<Integer>> {

    private static final long serialVersionUID = 1L;

    private String topic;




    public CustomKafkaSerializer(String topic)
    {
        this.topic = topic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(
            DecorateRecord<Integer> element, KafkaSinkContext context, Long timestamp) {



        try {
            return new ProducerRecord<>(
                    topic,
                    Strings.toByteArray(element.getPathInfo()) ,
                    Strings.toByteArray(String.format("%d-%d", element.getSeqNum(), Math.abs(element.getValue())))
            );
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "Could not serialize record: " + element, e);
        }
    }
}
