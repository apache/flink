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

package org.apache.flink.connectors.test.kafka.jobs;

import org.apache.flink.connectors.test.common.jobs.AbstractSourceJob;
import org.apache.flink.connectors.test.kafka.external.KafkaContainerizedExternalSystem;
import org.apache.flink.connectors.test.kafka.external.KafkaExternalContext;

import java.util.Properties;

/** Flink job for testing Kafka source connector. */
public class KafkaSourceJob extends AbstractSourceJob {
    public static void main(String[] args) throws Exception {
        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty("bootstrap.servers", "kafka:9092");
        kafkaProperties.setProperty("topic", KafkaContainerizedExternalSystem.TOPIC);
        (new KafkaSourceJob()).run(new KafkaExternalContext(kafkaProperties));
    }
}
