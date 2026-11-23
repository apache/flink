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
import org.apache.flink.table.api.TableRuntimeException;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;

import java.util.stream.Stream;

/** Tests for GEOGRAPHY constructor functions. */
class GeographyConstructorFunctionsITCase extends BuiltInFunctionTestBase {

    private static final byte[] POINT_WKB =
            new byte[] {
                1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, (byte) 0xF0, 0x3F, 0, 0, 0, 0, 0, 0, 0, 0x40
            };

    @Override
    Stream<TestSetSpec> getTestSetSpecs() {
        return Stream.of(stGeogFromTextCases(), stGeogFromWkbCases(), runtimeErrorCases());
    }

    private TestSetSpec stGeogFromTextCases() {
        return TestSetSpec.forFunction(BuiltInFunctionDefinitions.ST_GEOGFROMTEXT)
                .onFieldsWithData(null, "POINT (1 2)", "POINT EMPTY")
                .andDataTypes(DataTypes.STRING(), DataTypes.STRING().notNull(), DataTypes.STRING())
                .testSqlResult("ST_GEOGFROMTEXT(f0)", null, DataTypes.GEOGRAPHY())
                .testSqlResult("ST_GEOGFROMTEXT(f1)", DataTypes.GEOGRAPHY().notNull())
                .testSqlResult("ST_GEOGFROMTEXT(f2)", DataTypes.GEOGRAPHY());
    }

    private TestSetSpec stGeogFromWkbCases() {
        return TestSetSpec.forFunction(BuiltInFunctionDefinitions.ST_GEOGFROMWKB)
                .onFieldsWithData(null, POINT_WKB)
                .andDataTypes(DataTypes.BYTES(), DataTypes.BYTES().notNull())
                .testSqlResult("ST_GEOGFROMWKB(f0)", null, DataTypes.GEOGRAPHY())
                .testSqlResult("ST_GEOGFROMWKB(f1)", DataTypes.GEOGRAPHY().notNull());
    }

    private TestSetSpec runtimeErrorCases() {
        return TestSetSpec.forFunction(BuiltInFunctionDefinitions.ST_GEOGFROMTEXT, "Runtime errors")
                .onFieldsWithData(
                        "not wkt", "POINT Z (1 2 3)", "POINT (181 2)", new byte[] {1, 1, 0, 0})
                .andDataTypes(
                        DataTypes.STRING(),
                        DataTypes.STRING(),
                        DataTypes.STRING(),
                        DataTypes.BYTES())
                .testSqlRuntimeError(
                        "ST_GEOGFROMTEXT(f0)",
                        TableRuntimeException.class,
                        "Invalid GEOGRAPHY WKT.")
                .testSqlRuntimeError(
                        "ST_GEOGFROMTEXT(f1)",
                        TableRuntimeException.class,
                        "Only 2D coordinates are supported")
                .testSqlRuntimeError(
                        "ST_GEOGFROMTEXT(f2)",
                        TableRuntimeException.class,
                        "Expected range is [-180, 180]")
                .testSqlRuntimeError(
                        "ST_GEOGFROMWKB(f3)",
                        TableRuntimeException.class,
                        "Invalid GEOGRAPHY WKB.");
    }
}
