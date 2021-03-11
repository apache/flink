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

package org.apache.flink.connector.kafka.source;

import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/** Unite test class for {@link KafkaSourceBuilder}. */
public class KafkaSourceBuilderTest {

    @Test
    public void testParitionDiscoverySettingWithBounded() throws Exception {
        KafkaSourceBuilder sourceWithBounded =
                new KafkaSourceBuilder().setBounded(OffsetsInitializer.latest());
        sourceWithBounded.parseAndSetRequiredProperties();
        String actualWithBounded =
                sourceWithBounded
                        .getProps()
                        .getProperty(KafkaSourceOptions.PARTITION_DISCOVERY_INTERVAL_MS.key());
        String expectedWithBounded = "-1";
        assertEquals(expectedWithBounded, actualWithBounded);
    }

    @Test
    public void testParitionDiscoverySettingWithUnbounded() throws Exception {
        KafkaSourceBuilder sourceWithUnbounded =
                new KafkaSourceBuilder().setUnbounded(OffsetsInitializer.latest());
        sourceWithUnbounded.parseAndSetRequiredProperties();
        String actualWithUnbounded =
                sourceWithUnbounded
                        .getProps()
                        .getProperty(KafkaSourceOptions.PARTITION_DISCOVERY_INTERVAL_MS.key());
        String expectedWithUnbounded = "-1";
        assertEquals(expectedWithUnbounded, actualWithUnbounded);
    }

    @Test
    public void testParitionDiscoverySettingWithProps() throws Exception {
        KafkaSourceBuilder sourceWithProps =
                new KafkaSourceBuilder()
                        .setUnbounded(OffsetsInitializer.latest())
                        .setProperty(
                                KafkaSourceOptions.PARTITION_DISCOVERY_INTERVAL_MS.key(), "60000");
        sourceWithProps.parseAndSetRequiredProperties();
        String actualWithProps =
                sourceWithProps
                        .getProps()
                        .getProperty(KafkaSourceOptions.PARTITION_DISCOVERY_INTERVAL_MS.key());
        String expectedWithProps = "60000";
        assertEquals(expectedWithProps, actualWithProps);
    }
}
