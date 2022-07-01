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

package org.apache.flink.streaming.connectors.kinesis;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.flink.util.TestLogger;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link FlinkKinesisConsumer}. In contrast to tests in {@link FlinkKinesisConsumerTest}
 * it does not use power mock, which makes it possible to use e.g. the {@link ExpectedException}.
 */
public class KinesisConsumerTest extends TestLogger {

    @Test
    public void testKinesisConsumerThrowsExceptionIfSchemaImplementsCollector() {
        DeserializationSchema<Object> schemaWithCollector =
                new DeserializationSchema<Object>() {
                    @Override
                    public Object deserialize(byte[] message) throws IOException {
                        return null;
                    }

                    @Override
                    public void deserialize(byte[] message, Collector<Object> out)
                            throws IOException {
                        // we do not care about the implementation. we should just check if this
                        // method is declared
                    }

                    @Override
                    public boolean isEndOfStream(Object nextElement) {
                        return false;
                    }

                    @Override
                    public TypeInformation<Object> getProducedType() {
                        return null;
                    }
                };
        assertThatThrownBy(
                        () -> {
                            new FlinkKinesisConsumer<>(
                                    "fakeStream", schemaWithCollector, new Properties());
                        })
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "Kinesis consumer does not support DeserializationSchema that implements deserialization with a"
                                + " Collector. Unsupported DeserializationSchema: "
                                + "org.apache.flink.streaming.connectors.kinesis.KinesisConsumerTest");
    }
}
