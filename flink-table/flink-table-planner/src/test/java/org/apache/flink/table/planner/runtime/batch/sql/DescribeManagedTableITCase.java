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

package org.apache.flink.table.planner.runtime.batch.sql;

import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.planner.runtime.utils.BatchTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.table.factories.TestManagedTableFactory.BUCKET;
import static org.apache.flink.table.factories.TestManagedTableFactory.CHANGELOG_MODE;
import static org.apache.flink.table.factories.TestManagedTableFactory.MANAGED_TABLES;
import static org.apache.flink.table.factories.TestManagedTableFactory.MANIFEST_TARGET_FILE_SIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/** IT Case for testing describing managed table. */
public class DescribeManagedTableITCase extends BatchTestBase {

    private static final String TABLE_NAME = "test_managed_table";

    private final ObjectIdentifier tableIdentifier =
            ObjectIdentifier.of(
                    tEnv().getCurrentCatalog(), tEnv().getCurrentDatabase(), TABLE_NAME);

    @Override
    @Before
    public void before() {
        super.before();
        MANAGED_TABLES.put(tableIdentifier, new AtomicReference<>());
    }

    @Override
    @After
    public void after() {
        super.after();
        tEnv().executeSql(String.format("DROP TABLE %s", TABLE_NAME));
    }

    @Test
    public void testDescribeManagedNonPartitionedTable() {
        tEnv().executeSql(
                        String.format(
                                "CREATE TABLE %s (\n"
                                        + "  id BIGINT,\n"
                                        + "  content STRING\n"
                                        + ") WITH (\n"
                                        + " '%s' = 'I',\n"
                                        + " '%s' = '1'\n"
                                        + ")",
                                TABLE_NAME, CHANGELOG_MODE.key(), BUCKET.key()));
        CloseableIterator<Row> rowIterator =
                tEnv().executeSql(String.format("DESCRIBE EXTENDED %s", TABLE_NAME)).collect();
        assertEquals(Row.of(BUCKET.key()), rowIterator.next());
        assertEquals(Row.of(CHANGELOG_MODE.key()), rowIterator.next());
        assertFalse(rowIterator.hasNext());
    }

    @Test
    public void testDescribeManagedPartitionedTable() {
        tEnv().executeSql(
                        String.format(
                                "CREATE TABLE %s (\n"
                                        + "  id BIGINT,\n"
                                        + "  content STRING,\n"
                                        + "  season STRING\n"
                                        + ") PARTITIONED BY (season) WITH ("
                                        + " '%s' = 'I',\n"
                                        + " '%s' = '1'\n"
                                        + ")",
                                TABLE_NAME, CHANGELOG_MODE.key(), MANIFEST_TARGET_FILE_SIZE.key()));
        CloseableIterator<Row> rowIterator =
                tEnv().executeSql(String.format("DESCRIBE EXTENDED %s", TABLE_NAME)).collect();
        assertEquals(Row.of(MANIFEST_TARGET_FILE_SIZE.key()), rowIterator.next());
        assertEquals(Row.of(CHANGELOG_MODE.key()), rowIterator.next());
        assertFalse(rowIterator.hasNext());

        rowIterator =
                tEnv().executeSql(
                                String.format(
                                        "DESCRIBE EXTENDED %s PARTITION (season='spring')",
                                        TABLE_NAME))
                        .collect();
        assertEquals(Row.of(MANIFEST_TARGET_FILE_SIZE.key()), rowIterator.next());
        assertEquals(Row.of(CHANGELOG_MODE.key()), rowIterator.next());
        assertEquals(Row.of("partition"), rowIterator.next());
        assertFalse(rowIterator.hasNext());
    }
}
