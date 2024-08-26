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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.SpecializedFunction;
import org.apache.flink.table.types.DataType;

import javax.annotation.Nullable;

import java.lang.invoke.MethodHandle;

import static org.apache.flink.table.api.Expressions.$;

/** Implementation of {@link BuiltInFunctionDefinitions#DATEDIFF}. */
public class DatediffFunction extends BuiltInScalarFunction {

    private final SpecializedFunction.ExpressionEvaluator endCastEvaluator;
    private final SpecializedFunction.ExpressionEvaluator startCastEvaluator;

    private transient MethodHandle endCastHandle;
    private transient MethodHandle startCastHandle;

    public DatediffFunction(SpecializedFunction.SpecializedContext context) {
        super(BuiltInFunctionDefinitions.DATEDIFF, context);
        final DataType endDateDataType = context.getCallContext().getArgumentDataTypes().get(0);
        endCastEvaluator =
                context.createEvaluator(
                        $("endDate").cast(DataTypes.DATE().toInternal()),
                        DataTypes.DATE().toInternal(),
                        DataTypes.FIELD("endDate", endDateDataType.toInternal()));
        final DataType startDateDataType = context.getCallContext().getArgumentDataTypes().get(1);
        startCastEvaluator =
                context.createEvaluator(
                        $("startDate").cast(DataTypes.DATE().toInternal()),
                        DataTypes.DATE().toInternal(),
                        DataTypes.FIELD("startDate", startDateDataType.toInternal()));
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        endCastHandle = endCastEvaluator.open(context);
        startCastHandle = startCastEvaluator.open(context);
    }

    public @Nullable Integer eval(@Nullable Object endDate, @Nullable Object startDate) {
        if (endDate == null || startDate == null) {
            return null;
        }

        try {
            return (int) endCastHandle.invoke(endDate) - (int) startCastHandle.invoke(startDate);
        } catch (Throwable t) {
            return null;
        }
    }

    @Override
    public void close() throws Exception {
        endCastEvaluator.close();
        startCastEvaluator.close();
    }
}
