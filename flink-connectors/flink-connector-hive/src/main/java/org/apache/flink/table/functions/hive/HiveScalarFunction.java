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

package org.apache.flink.table.functions.hive;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.hive.util.HiveFunctionUtil;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.TypeInference;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

/**
 * Abstract class to provide more information for Hive {@link UDF} and {@link GenericUDF} functions.
 */
@Internal
public abstract class HiveScalarFunction<UDFType> extends ScalarFunction
        implements HiveFunction<UDFType> {

    protected final HiveFunctionWrapper<UDFType> hiveFunctionWrapper;

    protected HiveFunctionArguments arguments;

    protected transient UDFType function;
    protected transient ObjectInspector returnInspector;

    private transient boolean isArgsSingleArray;

    HiveScalarFunction(HiveFunctionWrapper<UDFType> hiveFunctionWrapper) {
        this.hiveFunctionWrapper = hiveFunctionWrapper;
    }

    @Override
    public boolean isDeterministic() {
        org.apache.hadoop.hive.ql.udf.UDFType udfType =
                hiveFunctionWrapper
                        .getUDFClass()
                        .getAnnotation(org.apache.hadoop.hive.ql.udf.UDFType.class);

        return udfType != null && udfType.deterministic() && !udfType.stateful();
    }

    @Override
    public void open(FunctionContext context) {
        openInternal();

        isArgsSingleArray = HiveFunctionUtil.isSingleBoxedArray(arguments);
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        return createTypeInference();
    }

    /** See {@link ScalarFunction#open(FunctionContext)}. */
    protected abstract void openInternal();

    public Object eval(Object... args) {

        // When the parameter is (Integer, Array[Double]), Flink calls udf.eval(Integer,
        // Array[Double]), which is not a problem.
        // But when the parameter is a single array, Flink calls udf.eval(Array[Double]),
        // at this point java's var-args will cast Array[Double] to Array[Object] and let it be
        // Object... args, So we need wrap it.
        if (isArgsSingleArray) {
            args = new Object[] {args};
        }

        return evalInternal(args);
    }

    /** Evaluation logical, args will be wrapped when is a single array. */
    protected abstract Object evalInternal(Object[] args);

    @Override
    public void setArguments(CallContext callContext) {
        arguments = HiveFunctionArguments.create(callContext);
    }

    @Override
    public HiveFunctionWrapper<UDFType> getFunctionWrapper() {
        return hiveFunctionWrapper;
    }
}
