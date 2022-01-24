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

import static org.apache.flink.util.Preconditions.checkState;

/**
 * Annotation that specifies a {@link KafkaClientKit} field in test class for interacting with
 * brokers in {@link KafkaExtension}.
 */
@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface KafkaKit {

    /** Validator of {@link KafkaKit}. */
    class Validator {
        public static void validate(Field annotatedField) {
            checkState(
                    annotatedField.getType().isAssignableFrom(KafkaKit.class),
                    String.format(
                            "Annotation %s is only applicable on field with type %s",
                            KafkaKit.class, KafkaClientKit.class));
        }
    }
}
