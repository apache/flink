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
import org.apache.flink.table.functions.SpecializedFunction.SpecializedContext;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.FlinkRuntimeException;

import javax.annotation.Nullable;

import java.lang.invoke.MethodHandle;

import static org.apache.flink.table.api.Expressions.$;

/** Implementation of {@link BuiltInFunctionDefinitions#DATE_SUB}. */
@Internal
public class DateSubFunction extends BuiltInScalarFunction {

    private final SpecializedFunction.ExpressionEvaluator castEvaluator;
    private transient MethodHandle castHandle;

    public DateSubFunction(SpecializedContext context) {
        super(BuiltInFunctionDefinitions.DATE_SUB, context);
        final DataType startDate = context.getCallContext().getArgumentDataTypes().get(0);
        castEvaluator =
                context.createEvaluator(
                        $("startDate").cast(DataTypes.DATE().notNull().bridgedTo(int.class)),
                        DataTypes.DATE().notNull().bridgedTo(int.class),
                        DataTypes.FIELD("startDate", startDate.notNull().toInternal()));
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        castHandle = castEvaluator.open(context);
    }

    public @Nullable Object eval(Object startDate, Object days) {
        try {
            if (startDate == null || days == null) {
                return null;
            }

            return (int) castHandle.invoke(startDate) - ((Number) days).intValue();
        } catch (Throwable t) {
            throw new FlinkRuntimeException(t);
        }
    }

    @Override
    public void close() throws Exception {
        castEvaluator.close();
    }
}
