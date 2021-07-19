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

package org.apache.flink.formats.thrift;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.formats.thrift.generated.Event;
import org.apache.flink.formats.thrift.generated.UserAction;
import org.apache.flink.formats.thrift.generated.UserEventType;

import org.apache.thrift.protocol.TCompactProtocol;
import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

/** Test for {@link ThriftDeserializationSchema} and {@link ThriftDeserializationSchema}. */
public class ThriftDeSerializationSchemaTest {

    @Test
    public void testDeSerialization() throws Exception {
        ThriftDeserializationSchema<Event> thriftDeserializationSchema =
                new ThriftDeserializationSchema<>(Event.class, TCompactProtocol.Factory.class);
        ThriftSerializationSchema<Event> thriftSerializationSchema =
                new ThriftSerializationSchema<>(TCompactProtocol.Factory.class);

        thriftDeserializationSchema.open(mock(DeserializationSchema.InitializationContext.class));
        thriftSerializationSchema.open(mock(SerializationSchema.InitializationContext.class));

        Event event =
                new Event()
                        .setEventType(UserEventType.BROWSE)
                        .setUserActions(
                                new HashMap<String, UserAction>() {
                                    {
                                        put(
                                                "action1",
                                                new UserAction()
                                                        .setTimestamp(0xffffL)
                                                        .setUrl("testUrl")
                                                        .setReferralUrl("testReferralUrl"));
                                    }
                                });
        byte[] bytes = thriftSerializationSchema.serialize(event);
        Event deserializeEvent = thriftDeserializationSchema.deserialize(bytes);

        assertTrue(deserializeEvent.equals(event));
    }
}
