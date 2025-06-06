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

package org.apache.flink.state.table;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.Test;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import static org.apache.flink.configuration.ExecutionOptions.RUNTIME_MODE;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for the savepoint metadata SQL reader. */
public class SavepointMetadataDynamicTableSourceTest {
    @Test
    public void testReadMetadata() throws Exception {
        Configuration config = new Configuration();
        config.set(RUNTIME_MODE, RuntimeExecutionMode.BATCH);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.executeSql("LOAD MODULE state");
        Table table =
                tEnv.sqlQuery("SELECT * FROM savepoint_metadata('src/test/resources/table-state')");
        List<Row> result = tEnv.toDataStream(table).executeAndCollect(100);
        result.sort(Comparator.comparing(a -> ((String) a.getField("operator-uid-hash"))));

        assertThat(result.size()).isEqualTo(7);
        Iterator<Row> it = result.iterator();
        assertThat(it.next().toString())
                .isEqualTo(
                        "+I[2, Source: broadcast-source, broadcast-source-uid, 3a6f51704798c4f418be51bfb6813b77, 1, 128, 0, 0, 0]");
        assertThat(it.next().toString())
                .isEqualTo(
                        "+I[2, keyed-broadcast-process, keyed-broadcast-process-uid, 413c1d6f88ee8627fe4b8bc533b4cf1b, 2, 128, 2, 0, 4548]");
    }
}
