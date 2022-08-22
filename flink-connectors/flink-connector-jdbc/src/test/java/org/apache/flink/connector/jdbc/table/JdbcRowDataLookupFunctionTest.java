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

package org.apache.flink.connector.jdbc.table;

import org.apache.flink.connector.jdbc.internal.options.JdbcConnectorOptions;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.source.lookup.LookupOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.connector.jdbc.JdbcTestFixture.DERBY_EBOOKSHOP_DB;
import static org.assertj.core.api.Assertions.assertThat;

/** Test suite for {@link JdbcRowDataLookupFunction}. */
public class JdbcRowDataLookupFunctionTest extends JdbcLookupTestBase {

    private static final String[] fieldNames = new String[] {"id1", "id2", "comment1", "comment2"};
    private static final DataType[] fieldDataTypes =
            new DataType[] {
                DataTypes.INT(), DataTypes.STRING(), DataTypes.STRING(), DataTypes.STRING()
            };

    private static final String[] lookupKeys = new String[] {"id1", "id2"};

    @ParameterizedTest(name = "withFailure = {0}")
    @ValueSource(booleans = {false, true})
    public void testLookup(boolean withFailure) throws Exception {
        JdbcRowDataLookupFunction lookupFunction = buildRowDataLookupFunction(withFailure);

        ListOutputCollector collector = new ListOutputCollector();
        lookupFunction.setCollector(collector);

        lookupFunction.open(null);

        lookupFunction.eval(1, StringData.fromString("1"));
        if (withFailure) {
            // Close connection here, and this will be recovered by retry
            if (lookupFunction.getDbConnection() != null) {
                lookupFunction.getDbConnection().close();
            }
        }
        lookupFunction.eval(2, StringData.fromString("3"));

        List<String> result =
                new ArrayList<>(collector.getOutputs())
                        .stream().map(RowData::toString).sorted().collect(Collectors.toList());

        List<String> expected = new ArrayList<>();
        expected.add("+I(1,1,11-c1-v1,11-c2-v1)");
        expected.add("+I(1,1,11-c1-v2,11-c2-v2)");
        expected.add("+I(2,3,null,23-c2)");
        Collections.sort(expected);

        assertThat(result).isEqualTo(expected);
    }

    private JdbcRowDataLookupFunction buildRowDataLookupFunction(boolean withFailure) {
        JdbcConnectorOptions jdbcOptions =
                JdbcConnectorOptions.builder()
                        .setDriverName(DERBY_EBOOKSHOP_DB.getDriverClass())
                        .setDBUrl(DB_URL)
                        .setTableName(LOOKUP_TABLE)
                        .build();

        RowType rowType =
                RowType.of(
                        Arrays.stream(fieldDataTypes)
                                .map(DataType::getLogicalType)
                                .toArray(LogicalType[]::new),
                        fieldNames);

        return new JdbcRowDataLookupFunction(
                jdbcOptions,
                withFailure ? 1 : LookupOptions.MAX_RETRIES.defaultValue(),
                fieldNames,
                fieldDataTypes,
                lookupKeys,
                rowType);
    }

    private static final class ListOutputCollector implements Collector<RowData> {

        private final List<RowData> output = new ArrayList<>();

        @Override
        public void collect(RowData row) {
            this.output.add(row);
        }

        @Override
        public void close() {}

        public List<RowData> getOutputs() {
            return output;
        }
    }
}
