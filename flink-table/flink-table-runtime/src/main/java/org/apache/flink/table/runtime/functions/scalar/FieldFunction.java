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

package org.apache.flink.table.runtime.functions.scalar;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.SpecializedFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static org.apache.flink.table.api.Expressions.$;

/** Implementation of {@link BuiltInFunctionDefinitions#FIELD}. */
@Internal
public class FieldFunction extends BuiltInScalarFunction {

    private final SpecializedFunction.ExpressionEvaluator equalityEvaluator;
    private transient MethodHandle equalityHandle;

    private final List<DataType> dataTypeList;

    public FieldFunction(SpecializedFunction.SpecializedContext context) {
        super(BuiltInFunctionDefinitions.FIELD, context);
        final List<DataType> argumentDataTypes = context.getCallContext().getArgumentDataTypes();
        dataTypeList = ImmutableList.copyOf(argumentDataTypes);
        final DataType dataType = context.getCallContext().getArgumentDataTypes().get(0);
        equalityEvaluator =
                context.createEvaluator(
                        $("element1").isEqual($("element2")),
                        DataTypes.BOOLEAN(),
                        DataTypes.FIELD("element1", dataType.notNull().toInternal()),
                        DataTypes.FIELD("element2", dataType.notNull().toInternal()));
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        equalityHandle = equalityEvaluator.open(context);
    }

    public @Nullable Integer eval(Object value, Object... array) {
        try {
            if (value == null || array == null || array.length == 0) {
                return 0;
            }
            LogicalTypeRoot dataType1 =
                    dataTypeList.get(0).notNull().toInternal().getLogicalType().getTypeRoot();
            if (dataType1.equals(LogicalTypeRoot.CHAR)
                    || dataType1.equals(LogicalTypeRoot.VARBINARY)) {
                dataType1 = LogicalTypeRoot.VARCHAR;
            }
            for (int i = 0; i < array.length; i++) {
                final Object element = array[i];
                if (element != null) {
                    LogicalTypeRoot dataType2 =
                            dataTypeList
                                    .get(i + 1)
                                    .notNull()
                                    .toInternal()
                                    .getLogicalType()
                                    .getTypeRoot();
                    if (dataType2.equals(LogicalTypeRoot.CHAR)
                            || dataType2.equals(LogicalTypeRoot.VARBINARY)) {
                        dataType2 = LogicalTypeRoot.VARCHAR;
                    }
                    if ((dataType1.equals(dataType2))
                            && (boolean) equalityHandle.invoke(element, value)) {
                        return i + 1;
                    }
                }
            }
            return 0;
        } catch (Throwable t) {
            throw new FlinkRuntimeException(t);
        }
    }

    @Override
    public void close() throws Exception {
        equalityEvaluator.close();
    }
}
