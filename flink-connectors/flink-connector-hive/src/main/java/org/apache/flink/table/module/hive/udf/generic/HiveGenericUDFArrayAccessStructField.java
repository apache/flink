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
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

import java.util.ArrayList;
import java.util.List;

/**
 * A custom UDF for supporting to access the field of struct data in an array by ".".
 *
 * <p>For example, such sql is allowed in Hive:
 *
 * <pre>{@code
 * create table t(a1 array<struct<f1:string>>);
 * select a1.f1 from t;
 * }</pre>
 *
 * <p>The behavior for it in Hive is to collect the value of field 'f1' for the struct data
 * contained in 'a1' to a list.
 */
public class HiveGenericUDFArrayAccessStructField extends GenericUDF {

    public static final String NAME = "flink_hive_array_access_struct_field";
    private final transient ArrayList<Object> ret = new ArrayList<>();
    private transient ListObjectInspector listOI;
    private transient StructObjectInspector structOI;
    private transient StructField structField;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length != 2) {
            throw new UDFArgumentException(
                    String.format("The function %s() accepts exactly 2 arguments.", NAME));
        }

        if (!(arguments[0] instanceof ListObjectInspector
                && ((ListObjectInspector) (arguments[0])).getListElementObjectInspector()
                        instanceof StructObjectInspector)) {
            throw new UDFArgumentException(
                    String.format(
                            "The function %s() takes an array of struct as first parameter.",
                            NAME));
        }

        if (!(arguments[1] instanceof ConstantObjectInspector)) {
            throw new UDFArgumentException(
                    String.format("The function %s() takes a constant as second parameter.", NAME));
        }
        listOI = (ListObjectInspector) arguments[0];
        structOI = (StructObjectInspector) listOI.getListElementObjectInspector();
        ConstantObjectInspector fieldOI = (ConstantObjectInspector) arguments[1];
        // get the field to be accessed
        structField = structOI.getStructFieldRef(fieldOI.getWritableConstantValue().toString());

        // return type is a list
        return ObjectInspectorFactory.getStandardListObjectInspector(
                structField.getFieldObjectInspector());
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        ret.clear();
        List<?> structs = listOI.getList(arguments[0].get());
        for (Object o : structs) {
            ret.add(structOI.getStructFieldData(o, structField));
        }
        return ret;
    }

    @Override
    public String getDisplayString(String[] children) {
        return HiveParserUtils.getStandardDisplayString(NAME, children);
    }
}
