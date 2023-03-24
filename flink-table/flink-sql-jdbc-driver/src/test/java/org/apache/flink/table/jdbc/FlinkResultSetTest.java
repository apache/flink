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

package org.apache.flink.table.jdbc;

import org.apache.flink.api.common.JobID;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ResultKind;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.client.gateway.StatementResult;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.sql.SQLDataException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

/** Tests for {@link FlinkResultSet}. */
public class FlinkResultSetTest {
    private static final int RECORD_SIZE = 5000;

    @Test
    public void testResultSet() throws Exception {
        List<Integer> dataList =
                IntStream.range(0, RECORD_SIZE).boxed().collect(Collectors.toList());
        CloseableIterator<RowData> data =
                CloseableIterator.adapterForIterator(
                        dataList.stream().map(i -> (RowData) GenericRowData.of(i)).iterator());
        List<Integer> resultList = new ArrayList<>();
        try (ResultSet resultSet =
                new FlinkResultSet(
                        new TestingStatement(),
                        new StatementResult(
                                ResolvedSchema.of(Column.physical("id", DataTypes.INT())),
                                data,
                                true,
                                ResultKind.SUCCESS,
                                JobID.generate()))) {
            while (resultSet.next()) {
                assertEquals(resultSet.getInt(1), resultSet.getInt("id"));
                resultList.add(resultSet.getInt(1));
                assertThrowsExactly(
                        SQLDataException.class,
                        () -> resultSet.getLong(1),
                        "java.lang.ClassCastException: java.lang.Integer cannot be cast to java.lang.Long");
                assertThrowsExactly(
                        SQLDataException.class,
                        () -> resultSet.getLong("id1"),
                        "Column[id1] is not exist");
            }
        }
        assertEquals(resultList, dataList);
    }
}
