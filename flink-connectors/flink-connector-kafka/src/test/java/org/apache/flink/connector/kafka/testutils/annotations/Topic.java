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

package org.apache.flink.connector.kafka.testutils.annotations;

import org.apache.flink.connector.kafka.testutils.extension.KafkaClientKit;
import org.apache.flink.connector.kafka.testutils.extension.KafkaExtension;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * Annotation for creating Kafka topics in tests using {@link KafkaExtension}.
 *
 * <p>This annotation should be used on {@link String} type field, which specifies the name of
 * topic. If the string is null or empty, a random UUID will be used as the topic name.
 *
 * <p>Lifecycle of the topic depends on whether the field is static or not: topic with static
 * keyword will be kept across test methods, while non-static topic will be created and destroyed
 * per method.
 *
 * <p>Example usages:
 *
 * <pre>
 * {@literal @}Kafka
 * class KafkaTest {
 *
 *     // Per-class topic
 *     {@literal @}Topic static final String TOPIC = "shared-topic";
 *
 *     // Per-method topic
 *     {@literal @}Topic String perMethodTopic = "perMethodTopic"
 *
 *     // Use a random UUID as topic name
 *     // After the topic is created, value of this field will be changed to
 *     // the actual topic name, e.g. cdf8cde2-7a89-11ec-90d6-0242ac120003
 *     {@literal @}Topic String randomTopic;
 *
 *     // Add a random UUID as suffix
 *     // After the topic is created, value of this field will be changed to
 *     // the actual topic name, e.g. prefix-cdf8cde2-7a89-11ec-90d6-0242ac120003
 *     {@literal @}Topic(randomizeName = true) String randomizedNameTopic = "prefix"
 *
 *     // Specify #partitions and replication factor manually
 *     {@literal @}Topic(numPartitions = 1, replicationFactor = 1) String singlePartitionTopic;
 * }
 * </pre>
 */
@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface Topic {

    int numPartitions() default KafkaClientKit.DEFAULT_NUM_PARTITIONS;

    short replicationFactor() default KafkaClientKit.DEFAULT_REPLICATION_FACTOR;

    boolean randomizeName() default false;

    /** Validator for checking the usage of {@link Topic} annotation. */
    class Validator {
        public static void validate(Topic topic, Field annotatedField) {
            checkState(
                    annotatedField.getType().isAssignableFrom(String.class),
                    String.format(
                            "Annotation %s is only applicable on field with type %s",
                            Topic.class, String.class));
            if (topic.randomizeName()) {
                checkState(
                        !Modifier.isFinal(annotatedField.getModifiers()),
                        "Cannot randomize topic name for field \"%s\" with final keyword",
                        annotatedField.getName());
            }
        }
    }
}
