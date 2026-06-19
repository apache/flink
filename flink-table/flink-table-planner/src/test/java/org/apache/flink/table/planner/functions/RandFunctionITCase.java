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

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.rand;
import static org.apache.flink.table.api.Expressions.randInteger;

/**
 * Test for {@link org.apache.flink.table.functions.BuiltInFunctionDefinitions#RAND} and {@link
 * org.apache.flink.table.functions.BuiltInFunctionDefinitions#RAND_INTEGER} and their return type.
 */
public class RandFunctionITCase extends BuiltInFunctionTestBase {

    @Override
    Stream<TestSetSpec> getTestSetSpecs() {
        return Stream.of(
                // RAND()
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.RAND)
                        .testSqlResult("RAND()", DataTypes.DOUBLE().notNull())
                        .testTableApiResult(rand(), DataTypes.DOUBLE().notNull()),
                // RAND(INT)
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.RAND)
                        .onFieldsWithData(10)
                        .andDataTypes(DataTypes.INT())
                        .testSqlResult("RAND(f0)", 0.7304302967434272, DataTypes.DOUBLE())
                        .testTableApiResult(rand($("f0")), 0.7304302967434272, DataTypes.DOUBLE()),
                // RAND(INT NOT NULL)
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.RAND)
                        .onFieldsWithData(10)
                        .andDataTypes(DataTypes.INT().notNull())
                        .testSqlResult("RAND(f0)", 0.7304302967434272, DataTypes.DOUBLE().notNull())
                        .testTableApiResult(
                                rand($("f0")), 0.7304302967434272, DataTypes.DOUBLE().notNull()),
                // RAND_INTEGER(INT)
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.RAND)
                        .onFieldsWithData(5)
                        .andDataTypes(DataTypes.INT())
                        .testSqlResult("RAND_INTEGER(f0)", DataTypes.INT())
                        .testTableApiResult(randInteger($("f0")), DataTypes.INT()),
                // RAND_INTEGER(INT NOT NULL)
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.RAND)
                        .onFieldsWithData(5)
                        .andDataTypes(DataTypes.INT().notNull())
                        .testSqlResult("RAND_INTEGER(f0)", DataTypes.INT().notNull())
                        .testTableApiResult(randInteger($("f0")), DataTypes.INT().notNull()),
                // RAND_INTEGER(INT, INT)
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.RAND)
                        .onFieldsWithData(5, 10)
                        .andDataTypes(DataTypes.INT(), DataTypes.INT())
                        .testSqlResult("RAND_INTEGER(f0, f1)", 7, DataTypes.INT())
                        .testTableApiResult(randInteger($("f0"), $("f1")), 7, DataTypes.INT()),
                // RAND_INTEGER(INT, INT NOT NULL)
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.RAND)
                        .onFieldsWithData(5, 10)
                        .andDataTypes(DataTypes.INT(), DataTypes.INT().notNull())
                        .testSqlResult("RAND_INTEGER(f0, f1)", 7, DataTypes.INT())
                        .testTableApiResult(randInteger($("f0"), $("f1")), 7, DataTypes.INT()),
                // RAND_INTEGER(INT NOT NULL, INT)
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.RAND)
                        .onFieldsWithData(5, 10)
                        .andDataTypes(DataTypes.INT().notNull(), DataTypes.INT())
                        .testSqlResult("RAND_INTEGER(f0, f1)", 7, DataTypes.INT())
                        .testTableApiResult(randInteger($("f0"), $("f1")), 7, DataTypes.INT()),
                // RAND_INTEGER(INT NOT NULL, INT NOT NULL)
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.RAND)
                        .onFieldsWithData(5, 10)
                        .andDataTypes(DataTypes.INT().notNull(), DataTypes.INT().notNull())
                        .testSqlResult("RAND_INTEGER(f0, f1)", 7, DataTypes.INT().notNull())
                        .testTableApiResult(
                                randInteger($("f0"), $("f1")), 7, DataTypes.INT().notNull()));
    }
}
