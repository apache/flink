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
import org.apache.flink.types.bitmap.Bitmap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.apache.flink.table.api.Expressions.$;

/** Test bitmap functions correct behaviour. */
class BitmapFunctionsITCase extends BuiltInFunctionTestBase {

    @Override
    Stream<TestSetSpec> getTestSetSpecs() {
        final List<TestSetSpec> specs = new ArrayList<>();
        specs.addAll(bitmapBuildTestCases());
        return specs.stream();
    }

    private List<TestSetSpec> bitmapBuildTestCases() {
        return Arrays.asList(
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.BITMAP_BUILD)
                        .onFieldsWithData(
                                null,
                                new Integer[] {1, null, 1},
                                new Integer[] {-1},
                                new Integer[] {1, 2, 3, -4})
                        .andDataTypes(
                                DataTypes.ARRAY(DataTypes.INT()),
                                DataTypes.ARRAY(DataTypes.INT()),
                                DataTypes.ARRAY(DataTypes.INT()),
                                DataTypes.ARRAY(DataTypes.INT()).notNull())
                        // null array
                        .testResult(
                                $("f0").bitmapBuild(), "BITMAP_BUILD(f0)", null, DataTypes.BITMAP())
                        // null array element
                        .testResult(
                                $("f1").arrayRemove(1).bitmapBuild(),
                                "BITMAP_BUILD(ARRAY_REMOVE(f1, 1))",
                                Bitmap.empty(),
                                DataTypes.BITMAP())
                        .testResult(
                                $("f1").bitmapBuild(),
                                "BITMAP_BUILD(f1)",
                                Bitmap.fromArray(new int[] {1}),
                                DataTypes.BITMAP())
                        // empty array
                        .testResult(
                                $("f2").arrayRemove(-1).bitmapBuild(),
                                "BITMAP_BUILD(ARRAY_REMOVE(f2, -1))",
                                Bitmap.empty(),
                                DataTypes.BITMAP())
                        // normal cases
                        .testResult(
                                $("f2").bitmapBuild(),
                                "BITMAP_BUILD(f2)",
                                Bitmap.fromArray(new int[] {-1}),
                                DataTypes.BITMAP())
                        .testResult(
                                $("f3").bitmapBuild(),
                                "BITMAP_BUILD(f3)",
                                Bitmap.fromArray(new int[] {1, 2, 3, -4}),
                                DataTypes.BITMAP().notNull()),
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.BITMAP_BUILD, "Validation Error")
                        .onFieldsWithData(1024, new long[] {1L, 2L})
                        .andDataTypes(DataTypes.INT(), DataTypes.ARRAY(DataTypes.BIGINT()))
                        .testTableApiValidationError(
                                $("f0").bitmapBuild(),
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "BITMAP_BUILD(array ARRAY<INT> NOT NULL)\n"
                                        + "BITMAP_BUILD(array ARRAY<INT>)")
                        .testSqlValidationError(
                                "BITMAP_BUILD(f1)",
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "BITMAP_BUILD(array ARRAY<INT> NOT NULL)\n"
                                        + "BITMAP_BUILD(array ARRAY<INT>)"));
    }
}
