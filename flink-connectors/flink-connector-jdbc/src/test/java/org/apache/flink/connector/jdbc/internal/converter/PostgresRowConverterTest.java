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

package org.apache.flink.connector.jdbc.internal.converter;

import org.apache.flink.connector.jdbc.converter.JdbcRowConverter;
import org.apache.flink.connector.jdbc.statement.FieldNamedPreparedStatement;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.Test;
import org.mockito.Mockito;

import java.sql.ResultSet;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;

/** Test for {@link PostgresRowConverter}. */
public class PostgresRowConverterTest {
    @Test
    public void testInternalConverterHandlesUUIDs() throws Exception {
        // arrange
        RowType rowType = RowType.of(new VarCharType(), new VarCharType());
        JdbcRowConverter rowConverter =
                new PostgresRowConverter(rowType) {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public String converterName() {
                        return "test";
                    }
                };

        UUID uuid = UUID.randomUUID();
        String notUUID = "not-a-uuid";

        ResultSet resultSet = Mockito.mock(ResultSet.class);
        Mockito.when(resultSet.getObject(1)).thenReturn(uuid);
        Mockito.when(resultSet.getObject(2)).thenReturn(notUUID);

        // act
        RowData res = rowConverter.toInternal(resultSet);

        // assert
        assertThat(res.getString(0).toString()).isEqualTo(uuid.toString());
        assertThat(res.getString(1).toString()).isEqualTo(notUUID);
    }

    @Test
    public void testExternalConverterHandlesUUIDs() throws Exception {
        // arrange
        RowType rowType = RowType.of(new VarCharType(), new VarCharType());
        JdbcRowConverter rowConverter = new PostgresRowConverter(rowType);

        UUID uuid = UUID.randomUUID();
        String notUUID = "not-a-uuid";

        RowData rowData =
                GenericRowData.of(
                        StringData.fromString(uuid.toString()), StringData.fromString(notUUID));

        FieldNamedPreparedStatement ps = Mockito.mock(FieldNamedPreparedStatement.class);

        // act
        rowConverter.toExternal(rowData, ps);

        // assert
        verify(ps).setObject(0, uuid);
        verify(ps).setString(1, notUUID);
    }
}
