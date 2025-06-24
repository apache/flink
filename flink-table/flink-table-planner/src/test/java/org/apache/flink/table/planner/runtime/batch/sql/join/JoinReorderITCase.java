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

package org.apache.flink.table.planner.runtime.batch.sql.join;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.planner.runtime.utils.JoinReorderITCaseBase;
import org.apache.flink.table.planner.utils.TestingTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowUtils;
import org.apache.flink.util.CollectionUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for JoinReorder in batch mode. */
public class JoinReorderITCase extends JoinReorderITCaseBase {

    @Override
    protected TableEnvironment getTableEnvironment() {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
        return TestingTableEnvironment.create(settings, null, TableConfig.getDefault());
    }

    @Override
    protected void assertEquals(String query, List<String> expectedList) {
        List<Row> rows = CollectionUtil.iteratorToList(tEnv.executeSql(query).collect());
        List<String> results;
        try {
            // As Setting RowUtils.USE_LEGACY_TO_STRING equals true, we only compare the content in
            // results, which will not include result rowKind.
            RowUtils.USE_LEGACY_TO_STRING = true;
            results = rows.stream().map(Row::toString).collect(Collectors.toList());
        } finally {
            RowUtils.USE_LEGACY_TO_STRING = false;
        }

        results = new ArrayList<>(results);
        results.sort(String::compareTo);
        expectedList.sort(String::compareTo);

        assertThat(results).isEqualTo(expectedList);
    }
}
