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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;

import java.util.stream.Stream;

/** Tests binary IN/NOT IN point sets without constant folding. */
class BinaryInPredicateITCase extends BuiltInFunctionTestBase {

    @Override
    Stream<TestSetSpec> getTestSetSpecs() {
        // Eight points exercise SEARCH's hash-set path instead of a short OR chain.
        final String points =
                "X'000001', X'000003', X'000005', X'000007', "
                        + "X'000009', X'00000B', X'00000D', X'80FF00'";
        return Stream.of(
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.IN)
                        .onFieldsWithData(
                                new byte[] {(byte) 0x80, (byte) 0xff, 0},
                                new byte[] {(byte) 0x80, (byte) 0xfe, 0},
                                null)
                        .andDataTypes(DataTypes.BYTES(), DataTypes.BYTES(), DataTypes.BYTES())
                        .testSqlResult("f0 IN (" + points + ")", true, DataTypes.BOOLEAN())
                        .testSqlResult("f0 NOT IN (" + points + ")", false, DataTypes.BOOLEAN())
                        .testSqlResult("f1 IN (" + points + ")", false, DataTypes.BOOLEAN())
                        .testSqlResult("f1 NOT IN (" + points + ")", true, DataTypes.BOOLEAN())
                        .testSqlResult(
                                "CAST(f0 AS BINARY(3)) IN (" + points + ")",
                                true,
                                DataTypes.BOOLEAN())
                        .testSqlResult("f2 IN (" + points + ")", null, DataTypes.BOOLEAN())
                        .testSqlResult("f2 NOT IN (" + points + ")", null, DataTypes.BOOLEAN())
                        .testSqlResult(
                                "f0 IN (" + points + ", CAST(NULL AS VARBINARY))",
                                true,
                                DataTypes.BOOLEAN())
                        .testSqlResult(
                                "f1 IN (" + points + ", CAST(NULL AS VARBINARY))",
                                null,
                                DataTypes.BOOLEAN())
                        .testSqlResult(
                                "f1 NOT IN (" + points + ", CAST(NULL AS VARBINARY))",
                                null,
                                DataTypes.BOOLEAN()));
    }
}
