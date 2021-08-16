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

package org.apache.flink.connector.pulsar.common.schema;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.connector.pulsar.testutils.SampleData.Bar;
import org.apache.flink.util.InstantiationUtil;

import org.apache.pulsar.client.api.Schema;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;

/** Unit tests for {@link PulsarSchemaTypeInformation}. */
class PulsarSchemaTypeInformationTest {

    @Test
    void pulsarTypeInfoSerializationAndCreation() throws Exception {
        PulsarSchema<Bar> schema = new PulsarSchema<>(Schema.AVRO(Bar.class), Bar.class);
        PulsarSchemaTypeInformation<Bar> info = new PulsarSchemaTypeInformation<>(schema);
        assertDoesNotThrow(() -> InstantiationUtil.clone(info));

        PulsarSchemaTypeInformation<Bar> clonedInfo = InstantiationUtil.clone(info);
        assertEquals(info, clonedInfo);
        assertNotSame(info, clonedInfo);

        assertDoesNotThrow(() -> info.createSerializer(new ExecutionConfig()));

        assertEquals(info.getTypeClass(), clonedInfo.getTypeClass());
    }
}
