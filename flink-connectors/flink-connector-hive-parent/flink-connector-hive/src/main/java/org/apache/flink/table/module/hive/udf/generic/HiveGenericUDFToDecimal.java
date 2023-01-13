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
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorConverter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableHiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;

/**
 * Counterpart of Hive's org.apache.hadoop.hive.ql.udf.generic.GenericUDFToDecimal, which removes
 * the method #setTypeInfo() used to pass target type for we have no way to pass target type to it
 * in Flink. Instead, the target type will be passed to the function as the second parameter.
 */
public class HiveGenericUDFToDecimal extends GenericUDF {

    public static final String NAME = "flink_hive_to_decimal";

    private transient PrimitiveObjectInspectorConverter.HiveDecimalConverter bdConverter;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 2) {
            throw new UDFArgumentLengthException(
                    "The function flink_hive_to_decimal requires exactly two arguments, got "
                            + arguments.length);
        }
        PrimitiveObjectInspector srcOI;
        try {
            srcOI = (PrimitiveObjectInspector) arguments[0];
        } catch (ClassCastException e) {
            throw new UDFArgumentException(
                    "The function flink_hive_to_decimal takes only primitive types as first argument.");
        }

        HiveDecimalObjectInspector targetOI;
        try {
            targetOI = (HiveDecimalObjectInspector) arguments[1];
        } catch (ClassCastException e) {
            throw new UDFArgumentException(
                    "The function flink_hive_to_decimal takes only decimal types as second argument.");
        }

        DecimalTypeInfo returnTypeInfo =
                new DecimalTypeInfo(targetOI.precision(), targetOI.scale());

        bdConverter =
                new PrimitiveObjectInspectorConverter.HiveDecimalConverter(
                        srcOI,
                        (SettableHiveDecimalObjectInspector)
                                PrimitiveObjectInspectorFactory
                                        .getPrimitiveWritableConstantObjectInspector(
                                                returnTypeInfo, null));

        return PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(returnTypeInfo);
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        Object o0 = arguments[0].get();
        if (o0 == null) {
            return null;
        }
        return bdConverter.convert(o0);
    }

    @Override
    public String getDisplayString(String[] children) {
        return HiveParserUtils.getStandardDisplayString("flink_hive_to_decimal", children);
    }
}
