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

package org.apache.flink.table.planner.delegation.hive.copy;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;

/** Add here what's missing in older version of hive's TypeInfoUtils. */
public class HiveParserTypeInfoUtils {

    public static List<PrimitiveObjectInspector.PrimitiveCategory> numericTypeList =
            new ArrayList<>();
    // The ordering of types here is used to determine which numeric types
    // are common/convertible to one another. Probably better to rely on the
    // ordering explicitly defined here than to assume that the enum values
    // that were arbitrarily assigned in PrimitiveCategory work for our purposes.
    public static EnumMap<PrimitiveObjectInspector.PrimitiveCategory, Integer> numericTypes =
            new EnumMap<>(PrimitiveObjectInspector.PrimitiveCategory.class);

    static {
        registerNumericType(PrimitiveObjectInspector.PrimitiveCategory.BYTE, 1);
        registerNumericType(PrimitiveObjectInspector.PrimitiveCategory.SHORT, 2);
        registerNumericType(PrimitiveObjectInspector.PrimitiveCategory.INT, 3);
        registerNumericType(PrimitiveObjectInspector.PrimitiveCategory.LONG, 4);
        registerNumericType(PrimitiveObjectInspector.PrimitiveCategory.DECIMAL, 5);
        registerNumericType(PrimitiveObjectInspector.PrimitiveCategory.FLOAT, 6);
        registerNumericType(PrimitiveObjectInspector.PrimitiveCategory.DOUBLE, 7);
        registerNumericType(PrimitiveObjectInspector.PrimitiveCategory.STRING, 8);
    }

    public static synchronized void registerNumericType(
            PrimitiveObjectInspector.PrimitiveCategory primitiveCategory, int level) {
        numericTypeList.add(primitiveCategory);
        numericTypes.put(primitiveCategory, level);
    }

    public static boolean implicitConvertible(TypeInfo from, TypeInfo to) {
        if (from.equals(to)) {
            return true;
        }

        // Reimplemented to use PrimitiveCategory rather than TypeInfo, because
        // 2 TypeInfos from the same qualified type (varchar, decimal) should still be
        // seen as equivalent.
        if (from.getCategory() == ObjectInspector.Category.PRIMITIVE
                && to.getCategory() == ObjectInspector.Category.PRIMITIVE) {
            return implicitConvertible(
                    ((PrimitiveTypeInfo) from).getPrimitiveCategory(),
                    ((PrimitiveTypeInfo) to).getPrimitiveCategory());
        }
        return false;
    }

    /** Test if it's implicitly convertible for data comparison. */
    public static boolean implicitConvertible(
            PrimitiveObjectInspector.PrimitiveCategory from,
            PrimitiveObjectInspector.PrimitiveCategory to) {
        if (from == to) {
            return true;
        }

        PrimitiveObjectInspectorUtils.PrimitiveGrouping fromPg =
                PrimitiveObjectInspectorUtils.getPrimitiveGrouping(from);
        PrimitiveObjectInspectorUtils.PrimitiveGrouping toPg =
                PrimitiveObjectInspectorUtils.getPrimitiveGrouping(to);

        // Allow implicit String to Double conversion
        if (fromPg == PrimitiveObjectInspectorUtils.PrimitiveGrouping.STRING_GROUP
                && to == PrimitiveObjectInspector.PrimitiveCategory.DOUBLE) {
            return true;
        }

        // Void can be converted to any type
        if (from == PrimitiveObjectInspector.PrimitiveCategory.VOID) {
            return true;
        }

        // Allow implicit String to Date conversion
        if (fromPg == PrimitiveObjectInspectorUtils.PrimitiveGrouping.DATE_GROUP
                && toPg == PrimitiveObjectInspectorUtils.PrimitiveGrouping.STRING_GROUP) {
            return true;
        }
        // Allow implicit Numeric to String conversion
        if (fromPg == PrimitiveObjectInspectorUtils.PrimitiveGrouping.NUMERIC_GROUP
                && toPg == PrimitiveObjectInspectorUtils.PrimitiveGrouping.STRING_GROUP) {
            return true;
        }
        // Allow implicit String to varchar conversion, and vice versa
        if (fromPg == PrimitiveObjectInspectorUtils.PrimitiveGrouping.STRING_GROUP
                && toPg == PrimitiveObjectInspectorUtils.PrimitiveGrouping.STRING_GROUP) {
            return true;
        }

        // Allow implicit conversion from Byte -> Integer -> Long -> Float -> Double
        // Decimal -> String
        Integer f = numericTypes.get(from);
        Integer t = numericTypes.get(to);
        if (f == null || t == null) {
            return false;
        }
        return f <= t;
    }
}
