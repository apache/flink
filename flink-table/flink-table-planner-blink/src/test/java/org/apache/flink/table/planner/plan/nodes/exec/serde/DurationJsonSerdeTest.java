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

package org.apache.flink.table.planner.plan.nodes.exec.serde;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.module.SimpleModule;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.io.StringWriter;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

/** Tests for {@link Duration} serialization and deserialization. */
@RunWith(Parameterized.class)
public class DurationJsonSerdeTest {

    @Parameterized.Parameter public Duration duration;

    @Test
    public void testDurationSerde() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addSerializer(new DurationJsonSerializer());
        module.addDeserializer(Duration.class, new DurationJsonDeserializer());
        mapper.registerModule(module);
        StringWriter writer = new StringWriter(100);
        try (JsonGenerator gen = mapper.getFactory().createGenerator(writer)) {
            gen.writeObject(duration);
        }
        String json = writer.toString();
        Duration actual = mapper.readValue(json, Duration.class);
        assertEquals(duration, actual);
    }

    @Parameterized.Parameters(name = "{0}")
    public static List<Duration> testData() {
        return Arrays.asList(
                Duration.ofNanos(1234567890),
                Duration.ofMillis(123456789),
                Duration.ofSeconds(12345),
                Duration.ofMinutes(123),
                Duration.ofHours(5),
                Duration.ofDays(10));
    }
}
