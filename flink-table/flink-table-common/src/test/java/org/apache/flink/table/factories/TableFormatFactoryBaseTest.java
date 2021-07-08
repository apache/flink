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

package org.apache.flink.table.factories;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.TableSchema;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/** Tests for {@link TableFormatFactoryBase}. */
public class TableFormatFactoryBaseTest {

    @Test
    public void testSchemaDerivation() {
        final Map<String, String> properties = new HashMap<>();
        properties.put("schema.0.name", "otherField");
        properties.put("schema.0.type", "VARCHAR");
        properties.put("schema.0.from", "csvField");
        properties.put("schema.1.name", "abcField");
        properties.put("schema.1.type", "VARCHAR");
        properties.put("schema.2.name", "p");
        properties.put("schema.2.type", "TIMESTAMP");
        properties.put("schema.2.proctime", "true");
        properties.put("schema.3.name", "r");
        properties.put("schema.3.type", "TIMESTAMP");
        properties.put("schema.3.rowtime.timestamps.type", "from-source");
        properties.put("schema.3.rowtime.watermarks.type", "from-source");

        final TableSchema actualSchema = TableFormatFactoryBase.deriveSchema(properties);
        final TableSchema expectedSchema =
                TableSchema.builder()
                        .field("csvField", Types.STRING) // aliased
                        .field("abcField", Types.STRING)
                        .build();
        assertEquals(expectedSchema, actualSchema);
    }

    @Test
    public void testSchemaDerivationWithRowtime() {
        final Map<String, String> properties = new HashMap<>();
        properties.put("schema.0.name", "otherField");
        properties.put("schema.0.type", "VARCHAR");
        properties.put("schema.0.from", "csvField");
        properties.put("schema.1.name", "abcField");
        properties.put("schema.1.type", "VARCHAR");
        properties.put("schema.2.name", "p");
        properties.put("schema.2.type", "TIMESTAMP");
        properties.put("schema.2.proctime", "true");
        properties.put("schema.3.name", "r");
        properties.put("schema.3.type", "TIMESTAMP");
        properties.put("schema.3.rowtime.timestamps.type", "from-field"); // from-field strategy
        properties.put("schema.3.rowtime.timestamps.from", "myTime");
        properties.put("schema.3.rowtime.watermarks.type", "from-source");

        final TableSchema actualSchema = TableFormatFactoryBase.deriveSchema(properties);
        final TableSchema expectedSchema =
                TableSchema.builder()
                        .field("csvField", Types.STRING) // aliased
                        .field("abcField", Types.STRING)
                        .field("myTime", Types.SQL_TIMESTAMP)
                        .build();
        assertEquals(expectedSchema, actualSchema);
    }
}
