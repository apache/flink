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

package org.apache.flink.connector.pulsar.table.testutils;

import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/** Util class for verify testing results. */
public class PulsarTableTestUtils {
    public static List<Row> collectRows(Table table, int expectedSize) throws Exception {
        final TableResult result = table.execute();
        final List<Row> collectedRows = new ArrayList<>();
        try (CloseableIterator<Row> iterator = result.collect()) {
            while (collectedRows.size() < expectedSize && iterator.hasNext()) {
                collectedRows.add(iterator.next());
            }
        }
        result.getJobClient()
                .ifPresent(
                        jc -> {
                            try {
                                jc.cancel().get(5, TimeUnit.SECONDS);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });

        return collectedRows;
    }
}
