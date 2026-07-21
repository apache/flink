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

/** Tests for GEOGRAPHY accessor functions. */
class GeographyAccessorFunctionsITCase extends BuiltInFunctionTestBase {

    private static final byte[] POINT_WKB =
            new byte[] {
                1, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, (byte) 0xF0, 0x3F, 0, 0, 0, 0, 0, 0, 0, 0x40
            };

    @Override
    Stream<TestSetSpec> getTestSetSpecs() {
        return Stream.of(stAsTextCases(), stAsWkbCases());
    }

    private TestSetSpec stAsTextCases() {
        return TestSetSpec.forFunction(BuiltInFunctionDefinitions.ST_ASTEXT)
                .onFieldsWithData(null, "POINT (1 2)", "POINT EMPTY")
                .andDataTypes(DataTypes.STRING(), DataTypes.STRING().notNull(), DataTypes.STRING())
                .testSqlResult("ST_ASTEXT(ST_GEOGFROMTEXT(f0))", null, DataTypes.STRING())
                .testSqlResult(
                        "ST_ASTEXT(ST_GEOGFROMTEXT(f1))",
                        "POINT (1 2)",
                        DataTypes.STRING().notNull())
                .testSqlResult("ST_ASTEXT(ST_GEOGFROMTEXT(f2))", "POINT EMPTY", DataTypes.STRING());
    }

    private TestSetSpec stAsWkbCases() {
        return TestSetSpec.forFunction(BuiltInFunctionDefinitions.ST_ASWKB)
                .onFieldsWithData(null, POINT_WKB)
                .andDataTypes(DataTypes.BYTES(), DataTypes.BYTES().notNull())
                .testSqlResult("ST_ASWKB(ST_GEOGFROMWKB(f0))", null, DataTypes.BYTES())
                .testSqlResult(
                        "ST_ASWKB(ST_GEOGFROMWKB(f1))", POINT_WKB, DataTypes.BYTES().notNull());
    }
}
