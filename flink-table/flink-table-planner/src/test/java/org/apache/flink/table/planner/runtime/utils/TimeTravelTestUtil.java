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

package org.apache.flink.table.planner.runtime.utils;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.planner.factories.TestTimeTravelCatalog;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.utils.DateTimeTestUtil;
import org.apache.flink.types.Row;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Test data for time travel. */
public class TimeTravelTestUtil {
    public static final List<Tuple3<String, Schema, List<Row>>> TEST_TIME_TRAVEL_DATE =
            Arrays.asList(
                    Tuple3.of(
                            "2023-01-01 01:00:00",
                            Schema.newBuilder().column("f1", DataTypes.INT()).build(),
                            Collections.singletonList(Row.of(1))),
                    Tuple3.of(
                            "2023-01-01 02:00:00",
                            Schema.newBuilder()
                                    .column("f1", DataTypes.INT())
                                    .column("f2", DataTypes.INT())
                                    .build(),
                            Collections.singletonList(Row.of(1, 2))),
                    Tuple3.of(
                            "2023-01-01 03:00:00",
                            Schema.newBuilder()
                                    .column("f1", DataTypes.INT())
                                    .column("f2", DataTypes.INT())
                                    .column("f3", DataTypes.INT())
                                    .build(),
                            Collections.singletonList(Row.of(1, 2, 3))));

    public static TestTimeTravelCatalog getTestingCatalogWithVersionedTable(
            String catalogName, String tableName) {
        TestTimeTravelCatalog catalog = new TestTimeTravelCatalog(catalogName);

        TEST_TIME_TRAVEL_DATE.forEach(
                t -> {
                    String dataId = TestValuesTableFactory.registerData(t.f2);
                    Map<String, String> options = new HashMap<>();
                    options.put("connector", "values");
                    options.put("bounded", "true");
                    options.put("data-id", dataId);
                    try {
                        catalog.registerTableForTimeTravel(
                                tableName,
                                t.f1,
                                options,
                                DateTimeTestUtil.toEpochMills(
                                        t.f0, "yyyy-MM-dd HH:mm:ss", ZoneId.of("UTC")));
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
        return catalog;
    }
}
