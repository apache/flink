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

package org.apache.flink.table.planner.functions;

import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.types.Row;

import java.util.stream.Stream;

import static java.util.Collections.singletonMap;
import static org.apache.flink.table.api.DataTypes.ARRAY;
import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.MAP;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.Expressions.$;

/**
 * Regular tests. See also {@link ConstructedAccessFunctionsITCase} for tests that access a nested
 * field of an expression or for {@link BuiltInFunctionDefinitions#FLATTEN} which produces multiple
 * columns from a single one.
 */
class FieldAccessFromTableITCase extends BuiltInFunctionTestBase {

    @Override
    Stream<TestSetSpec> getTestSetSpecs() {
        return Stream.of(
                // for a nullable structured type we should enforce nullable for all inner fields,
                // that is to say, not null is invalid for inner fields in such cases, more details
                // see FLINK-31830

                // Actually in case of SQL it does not use the GET method, but
                // a custom logic for accessing nested fields of a Table.
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.GET)
                        .onFieldsWithData(null, Row.of(1))
                        .andDataTypes(
                                ROW(FIELD("nested", BIGINT().nullable())).nullable(),
                                ROW(FIELD("nested", BIGINT().notNull())).notNull())
                        .testResult($("f0").get("nested"), "f0.nested", null, BIGINT().nullable())
                        .testResult($("f1").get("nested"), "f1.nested", 1L, BIGINT().notNull()),

                // In Calcite it maps to FlinkSqlOperatorTable.ITEM
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.AT)
                        .onFieldsWithData(
                                null,
                                new int[] {1},
                                null,
                                singletonMap("nested", 1),
                                null,
                                Row.of(1))
                        .andDataTypes(
                                ARRAY(BIGINT().nullable()).nullable(),
                                ARRAY(BIGINT().notNull()).notNull(),
                                MAP(STRING(), BIGINT().nullable()).nullable(),
                                MAP(STRING(), BIGINT().notNull()).notNull(),
                                ROW(FIELD("nested", BIGINT().nullable())).nullable(),
                                ROW(FIELD("nested", BIGINT().notNull())).notNull())
                        // accessing elements of MAP or ARRAY is a runtime operations,
                        // we do not know about the size or contents during the inference
                        // therefore the results are always nullable
                        .testResult($("f0").at(1), "f0[1]", null, BIGINT().nullable())
                        .testResult($("f1").at(1), "f1[1]", 1L, BIGINT().nullable())
                        .testResult($("f2").at("nested"), "f2['nested']", null, BIGINT().nullable())
                        .testResult($("f3").at("nested"), "f3['nested']", 1L, BIGINT().nullable())

                        // we know all the fields of a type up front, therefore we can
                        // derive more accurate types during the inference
                        .testResult(
                                $("f4").get("nested"), "f4['nested']", null, BIGINT().nullable())
                        .testResult($("f5").get("nested"), "f5['nested']", 1L, BIGINT().notNull()));
    }
}
