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

package org.apache.flink.table.catalog;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link DefaultCatalogTable}. */
class DefaultCatalogTableTest {

    @Test
    void toStringRedactsSensitiveOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("connector", "kafka");
        options.put("password", "topsecret");
        options.put("my.token", "tok123");

        CatalogTable table =
                CatalogTable.newBuilder()
                        .schema(Schema.newBuilder().column("id", DataTypes.INT()).build())
                        .options(options)
                        .build();

        String result = table.toString();

        assertThat(result).contains("connector=kafka");
        assertThat(result).doesNotContain("topsecret");
        assertThat(result).doesNotContain("tok123");
        assertThat(result).contains("password=******");
        assertThat(result).contains("my.token=******");
    }

    @Test
    void toStringKeepsNonSensitiveOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("connector", "kafka");
        options.put("topic", "my-topic");

        CatalogTable table =
                CatalogTable.newBuilder()
                        .schema(Schema.newBuilder().column("id", DataTypes.INT()).build())
                        .options(options)
                        .build();

        String result = table.toString();

        assertThat(result).contains("connector=kafka");
        assertThat(result).contains("topic=my-topic");
    }

    @Test
    void toStringWithEmptyOptionsDoesNotFail() {
        CatalogTable table =
                CatalogTable.newBuilder()
                        .schema(Schema.newBuilder().column("id", DataTypes.INT()).build())
                        .options(Map.of())
                        .build();

        assertThat(table.toString()).contains("options={}");
    }
}
