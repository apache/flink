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

package org.apache.flink.table.module.hive.udf.generic;

import org.apache.flink.table.planner.delegation.hive.HiveParserUtils;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.io.LongWritable;

import java.util.BitSet;

/**
 * Hive's GROUPING__ID changed since 2.3.0. This function is to convert the new GROUPING__ID to the
 * legacy value for older Hive versions. See https://issues.apache.org/jira/browse/HIVE-16102
 */
public class GenericUDFLegacyGroupingID extends GenericUDF {

    public static final String NAME = "_legacy_grouping__id";

    private transient PrimitiveObjectInspector groupingIdOI;
    private int numExprs;
    private final LongWritable legacyValue = new LongWritable();

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        // we accept two arguments: the new GROUPING__ID and the number of GBY expressions
        if (arguments.length != 2) {
            throw new UDFArgumentLengthException(
                    "Expect 2 arguments but actually got " + arguments.length);
        }
        if (arguments[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(0, "First argument should be primitive type");
        }
        if (arguments[1].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(1, "Second argument should be primitive type");
        }
        groupingIdOI = (PrimitiveObjectInspector) arguments[0];
        if (groupingIdOI.getPrimitiveCategory()
                != PrimitiveObjectInspector.PrimitiveCategory.LONG) {
            throw new UDFArgumentTypeException(0, "First argument should be a long");
        }
        PrimitiveObjectInspector numExprOI = (PrimitiveObjectInspector) arguments[1];
        if (numExprOI.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.INT) {
            throw new UDFArgumentTypeException(1, "Second argument should be an int");
        }
        if (!(numExprOI instanceof ConstantObjectInspector)) {
            throw new UDFArgumentTypeException(1, "Second argument should be a constant");
        }
        numExprs =
                PrimitiveObjectInspectorUtils.getInt(
                        ((ConstantObjectInspector) numExprOI).getWritableConstantValue(),
                        numExprOI);
        if (numExprs < 1 || numExprs > 64) {
            throw new UDFArgumentException(
                    "Number of GROUP BY expressions out of range: " + numExprs);
        }
        return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        long groupingId = PrimitiveObjectInspectorUtils.getLong(arguments[0].get(), groupingIdOI);
        BitSet bitSet = BitSet.valueOf(new long[] {groupingId});
        // flip each bit
        bitSet.flip(0, numExprs);
        // reverse bit order
        int i = 0;
        int j = numExprs - 1;
        while (i < j) {
            bitSet.set(i, bitSet.get(i) ^ bitSet.get(j));
            bitSet.set(j, bitSet.get(i) ^ bitSet.get(j));
            bitSet.set(i, bitSet.get(i) ^ bitSet.get(j));
            i++;
            j--;
        }
        long[] words = bitSet.toLongArray();
        legacyValue.set(words.length == 0 ? 0L : words[0]);
        return legacyValue;
    }

    @Override
    public String getDisplayString(String[] children) {
        return HiveParserUtils.getStandardDisplayString("grouping", children);
    }
}
