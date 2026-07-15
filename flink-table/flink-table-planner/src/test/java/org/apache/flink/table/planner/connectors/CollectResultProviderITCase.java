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

package org.apache.flink.table.planner.connectors;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.CollectionUtil;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * ITCase for collecting SELECT results via the Table API (backed by {@code
 * CollectDynamicSink.CollectResultProvider}).
 */
@ExtendWith(MiniClusterExtension.class)
@Timeout(value = 30, unit = TimeUnit.SECONDS)
class CollectResultProviderITCase {

    private static final String EMPTY_RESULT_QUERY =
            "SELECT * FROM (VALUES (1)) AS t(x) WHERE x < 0";

    @Test
    void awaitAndCollectCompleteForEmptyBatchResult() throws Exception {
        final TableEnvironment tEnv =
                TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());

        final TableResult result = tEnv.executeSql(EMPTY_RESULT_QUERY);

        result.await();
        try (CloseableIterator<Row> rows = result.collect()) {
            assertThat(rows.hasNext()).isFalse();
        }
    }

    @Test
    void awaitAndCollectCompleteForNonEmptyBatchResult() throws Exception {
        final TableEnvironment tEnv =
                TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());

        final TableResult result =
                tEnv.executeSql("SELECT x FROM (VALUES (1), (2), (3)) AS t(x) WHERE x > 1");

        result.await();
        try (CloseableIterator<Row> rows = result.collect()) {
            assertThat(CollectionUtil.iteratorToList(rows))
                    .containsExactlyInAnyOrder(Row.of(2), Row.of(3));
        }
    }

    @Test
    void awaitAndCollectCompleteForEmptyStreamingResult() throws Exception {
        final TableEnvironment tEnv =
                TableEnvironment.create(
                        EnvironmentSettings.newInstance().inStreamingMode().build());

        final TableResult result = tEnv.executeSql(EMPTY_RESULT_QUERY);

        result.await();
        try (CloseableIterator<Row> rows = result.collect()) {
            assertThat(rows.hasNext()).isFalse();
        }
    }
}
