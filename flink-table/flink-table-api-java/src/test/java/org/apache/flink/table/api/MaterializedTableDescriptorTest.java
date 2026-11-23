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

package org.apache.flink.table.api;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link MaterializedTableDescriptor}. */
class MaterializedTableDescriptorTest {

    private static final ConfigOption<Integer> PARALLELISM =
            ConfigOptions.key("sink.parallelism").intType().noDefaultValue();

    @Test
    void testToString() {
        assertThat(fullDescriptor().toString())
                .isEqualTo(
                        "(\n"
                                + "  `k` INT NOT NULL,\n"
                                + "  `cnt` BIGINT,\n"
                                + "  CONSTRAINT `PK_k` PRIMARY KEY (`k`) NOT ENFORCED\n"
                                + ")\n"
                                + "COMMENT 'it''s a comment'\n"
                                + "DISTRIBUTED BY HASH(`k`) INTO 2 BUCKETS\n"
                                + "PARTITIONED BY (`k`)\n"
                                + "WITH (\n"
                                + "  'connector' = 'values',\n"
                                + "  'sink.parallelism' = '4'\n"
                                + ")\n"
                                + "FRESHNESS = INTERVAL '1' MINUTE\n"
                                + "REFRESH_MODE = CONTINUOUS");
        assertThat(MaterializedTableDescriptor.newBuilder().build().toString()).isEmpty();
    }

    @Test
    void testToBuilderAndEquals() {
        final MaterializedTableDescriptor descriptor = fullDescriptor();

        assertThat(descriptor.toBuilder().build())
                .isEqualTo(descriptor)
                .hasSameHashCodeAs(descriptor);
        assertThat(descriptor.toBuilder().comment("other").build()).isNotEqualTo(descriptor);
        assertThat(descriptor.toBuilder().fullRefresh().build()).isNotEqualTo(descriptor);
    }

    private static MaterializedTableDescriptor fullDescriptor() {
        return MaterializedTableDescriptor.newBuilder()
                .schema(
                        Schema.newBuilder()
                                .column("k", DataTypes.INT().notNull())
                                .column("cnt", DataTypes.BIGINT())
                                .primaryKey("k")
                                .build())
                .comment("it's a comment")
                .distributedByHash(2, "k")
                .partitionedBy("k")
                .option("connector", "values")
                .option(PARALLELISM, 4)
                .freshness(Duration.ofMinutes(1))
                .continuousRefresh()
                .build();
    }
}
