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

package org.apache.flink.streaming.connectors.kinesis.serialization;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * Tests for the {@link KinesisDeserializationSchemaWrapper} using Deserialization schemas with and
 * without a Collector.
 */
public class KinesisDeserializationSchemaWrapperTest {

    @Test
    public void testKinesisConsumerRecognizesSchemaWithCollector() {
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
        KinesisDeserializationSchemaWrapper<Object> wrapper =
                new KinesisDeserializationSchemaWrapper(schemaWithCollector);
        Assert.assertTrue(wrapper.isUseCollector());
    }

    @Test
    public void testKinesisConsumerRecognizesSchemaWithoutCollector() {
        DeserializationSchema<Object> schemaWithCollector =
                new DeserializationSchema<Object>() {
                    @Override
                    public Object deserialize(byte[] message) throws IOException {
                        return null;
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
        KinesisDeserializationSchemaWrapper<Object> wrapper =
                new KinesisDeserializationSchemaWrapper(schemaWithCollector);
        Assert.assertFalse(wrapper.isUseCollector());
    }
}
