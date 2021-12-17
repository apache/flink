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

package org.apache.flink.table.functions.hive.util;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.IntWritable;

import static org.apache.flink.util.Preconditions.checkArgument;

/** Test generic udf. */
public class TestHiveGenericUDF extends GenericUDF {

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        checkArgument(arguments.length == 2);

        // TEST for constant arguments
        checkArgument(arguments[1] instanceof ConstantObjectInspector);
        Object constant = ((ConstantObjectInspector) arguments[1]).getWritableConstantValue();
        checkArgument(constant instanceof IntWritable);
        checkArgument(((IntWritable) constant).get() == 1);

        if (arguments[0] instanceof IntObjectInspector
                || arguments[0] instanceof StringObjectInspector) {
            return arguments[0];
        } else {
            throw new RuntimeException("Not support argument: " + arguments[0]);
        }
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        return arguments[0].get();
    }

    @Override
    public String getDisplayString(String[] children) {
        return "TestHiveGenericUDF";
    }
}
