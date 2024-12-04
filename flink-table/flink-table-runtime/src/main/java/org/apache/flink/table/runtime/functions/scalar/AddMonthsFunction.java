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
import org.apache.flink.table.utils.DateTimeUtils;

import javax.annotation.Nullable;

import java.lang.invoke.MethodHandle;

import static org.apache.flink.table.api.Expressions.$;

/** Implementation of {@link BuiltInFunctionDefinitions#ADD_MONTHS}. */
public class AddMonthsFunction extends BuiltInScalarFunction {

    private final SpecializedFunction.ExpressionEvaluator castEvaluator;
    private transient MethodHandle castHandle;

    public AddMonthsFunction(SpecializedFunction.SpecializedContext context) {
        super(BuiltInFunctionDefinitions.ADD_MONTHS, context);
        final DataType startDateDataType = context.getCallContext().getArgumentDataTypes().get(0);
        castEvaluator =
                context.createEvaluator(
                        $("startDate").cast(DataTypes.DATE().toInternal()),
                        DataTypes.DATE().toInternal(),
                        DataTypes.FIELD("startDate", startDateDataType.toInternal()));
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        castHandle = castEvaluator.open(context);
    }

    public @Nullable Integer eval(@Nullable Object startDate, @Nullable Number numMonths) {
        if (startDate == null
                || numMonths == null
                || numMonths.longValue() > Integer.MAX_VALUE
                || numMonths.longValue() < Integer.MIN_VALUE) {
            return null;
        }

        try {
            int resultDate =
                    DateTimeUtils.addMonths(
                            (int) castHandle.invoke(startDate), numMonths.intValue());
            return DateTimeUtils.isIllegalDate(resultDate) ? null : resultDate;
        } catch (Throwable t) {
            return null;
        }
    }

    @Override
    public void close() throws Exception {
        castEvaluator.close();
    }
}
