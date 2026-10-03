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

package org.apache.flink.state.api.schema;

import org.apache.flink.state.catalog.StateCatalog;
import org.apache.flink.state.table.module.StateModule;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Shared test helpers for standing up a {@link TableEnvironment} backed by a {@link StateCatalog}
 * and running queries against it, used by both {@link StateCatalogNonKeyedITCase} and {@link
 * StateCatalogWindowITCase}.
 */
final class StateCatalogTestUtils {

    private StateCatalogTestUtils() {}

    static TableEnvironment newTableEnv() {
        TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        tableEnv.loadModule("state", StateModule.INSTANCE);
        return tableEnv;
    }

    static StateCatalog registerCatalog(TableEnvironment tableEnv, String savepointPath) {
        StateCatalog catalog =
                new StateCatalog("state", Collections.singletonMap("test", savepointPath));
        catalog.open();
        tableEnv.registerCatalog("state", catalog);
        return catalog;
    }

    static List<Row> collect(TableEnvironment tableEnv, String sql) throws Exception {
        Table table = tableEnv.sqlQuery(sql);
        List<Row> rows = new ArrayList<>();
        try (CloseableIterator<Row> it = table.execute().collect()) {
            it.forEachRemaining(rows::add);
        }
        return rows;
    }
}
