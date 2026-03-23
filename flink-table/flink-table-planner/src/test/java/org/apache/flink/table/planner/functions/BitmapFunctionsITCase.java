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
import org.apache.flink.types.bitmap.Bitmap;

import org.roaringbitmap.RoaringBitmap;

import java.nio.ByteBuffer;
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
        specs.addAll(bitmapFromBytesTestCases());
        specs.addAll(bitmapToBytesTestCases());
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

    private List<TestSetSpec> bitmapFromBytesTestCases() {
        return Arrays.asList(
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.BITMAP_FROM_BYTES)
                        .onFieldsWithData(
                                null,
                                toSerializedBytes(),
                                toSerializedBytes(-1),
                                toSerializedBytes(1, 2, 3, -4))
                        .andDataTypes(
                                DataTypes.BYTES(),
                                DataTypes.BYTES(),
                                DataTypes.BYTES(),
                                DataTypes.BYTES().notNull())
                        // null
                        .testResult(
                                $("f0").bitmapFromBytes(),
                                "BITMAP_FROM_BYTES(f0)",
                                null,
                                DataTypes.BITMAP())
                        // empty
                        .testResult(
                                $("f1").bitmapFromBytes(),
                                "BITMAP_FROM_BYTES(f1)",
                                Bitmap.empty(),
                                DataTypes.BITMAP())
                        // normal cases
                        .testResult(
                                $("f2").bitmapFromBytes(),
                                "BITMAP_FROM_BYTES(f2)",
                                Bitmap.fromArray(new int[] {-1}),
                                DataTypes.BITMAP())
                        .testResult(
                                $("f3").bitmapFromBytes(),
                                "BITMAP_FROM_BYTES(f3)",
                                Bitmap.fromArray(new int[] {1, 2, 3, -4}),
                                DataTypes.BITMAP().notNull()),
                TestSetSpec.forFunction(
                                BuiltInFunctionDefinitions.BITMAP_FROM_BYTES, "Runtime Error")
                        .onFieldsWithData("".getBytes(), "invalid".getBytes())
                        .andDataTypes(DataTypes.BYTES(), DataTypes.BYTES())
                        .testTableApiRuntimeError(
                                $("f0").bitmapFromBytes(),
                                TableRuntimeException.class,
                                "Failed to deserialize bitmap from bytes.")
                        .testSqlRuntimeError(
                                "BITMAP_FROM_BYTES(f1)",
                                TableRuntimeException.class,
                                "Failed to deserialize bitmap from bytes."),
                TestSetSpec.forFunction(
                                BuiltInFunctionDefinitions.BITMAP_FROM_BYTES, "Validation Error")
                        .onFieldsWithData("{1,2}", new int[] {1, 2})
                        .andDataTypes(DataTypes.STRING(), DataTypes.ARRAY(DataTypes.INT()))
                        .testTableApiValidationError(
                                $("f0").bitmapFromBytes(),
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "BITMAP_FROM_BYTES(bytes <BINARY_STRING>)")
                        .testSqlValidationError(
                                "BITMAP_FROM_BYTES(f1)",
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "BITMAP_FROM_BYTES(bytes <BINARY_STRING>)"));
    }

    private List<TestSetSpec> bitmapToBytesTestCases() {
        return Arrays.asList(
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.BITMAP_TO_BYTES)
                        .onFieldsWithData(
                                null,
                                new Integer[] {-1},
                                new Integer[] {Integer.MIN_VALUE, -1, 1, 2, 3})
                        .andDataTypes(
                                DataTypes.ARRAY(DataTypes.INT()),
                                DataTypes.ARRAY(DataTypes.INT()),
                                DataTypes.ARRAY(DataTypes.INT()).notNull())
                        // null
                        .testResult(
                                $("f0").bitmapBuild().bitmapToBytes(),
                                "BITMAP_TO_BYTES(BITMAP_BUILD(f0))",
                                null,
                                DataTypes.BYTES())
                        // empty
                        .testResult(
                                $("f1").arrayRemove(-1).bitmapBuild().bitmapToBytes(),
                                "BITMAP_TO_BYTES(BITMAP_BUILD(ARRAY_REMOVE(f1, -1)))",
                                toSerializedBytes(),
                                DataTypes.BYTES())
                        // normal cases
                        .testResult(
                                $("f1").bitmapBuild().bitmapToBytes(),
                                "BITMAP_TO_BYTES(BITMAP_BUILD(f1))",
                                toSerializedBytes(-1),
                                DataTypes.BYTES())
                        .testResult(
                                $("f2").bitmapBuild().bitmapToBytes(),
                                "BITMAP_TO_BYTES(BITMAP_BUILD(f2))",
                                toSerializedBytes(1, 2, 3, Integer.MIN_VALUE, -1),
                                DataTypes.BYTES().notNull()),
                TestSetSpec.forFunction(
                                BuiltInFunctionDefinitions.BITMAP_TO_BYTES, "Validation Error")
                        .onFieldsWithData(1024, new int[] {1, 2})
                        .andDataTypes(DataTypes.INT(), DataTypes.ARRAY(DataTypes.INT()))
                        .testTableApiValidationError(
                                $("f0").bitmapToBytes(),
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "BITMAP_TO_BYTES(bitmap <BITMAP>)")
                        .testSqlValidationError(
                                "BITMAP_TO_BYTES(f1)",
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "BITMAP_TO_BYTES(bitmap <BITMAP>)"));
    }

    // ~ Utils --------------------------------------------------------------------

    private byte[] toSerializedBytes(int... values) {
        assert values != null;
        RoaringBitmap rb = RoaringBitmap.bitmapOf(values);
        rb.runOptimize();
        ByteBuffer buffer = ByteBuffer.allocate(rb.serializedSizeInBytes());
        rb.serialize(buffer);
        return buffer.array();
    }
}
