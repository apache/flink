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
 *
 */

package org.apache.flink.table.runtime.util;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.SpecializedFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.FlinkRuntimeException;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.lang.invoke.MethodHandle;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.expressions.ApiExpressionUtils.unresolvedCall;

/**
 * This class is used for scalar function, in that it is used for sharing the initialization context
 * between scalar functions that need code generated hashcode and equals method.
 */
@Internal
public class EqualityAndHashcodeProvider implements Closeable, Serializable {
    private final SpecializedFunction.ExpressionEvaluator hashcodeEvaluator;
    private final SpecializedFunction.ExpressionEvaluator equalityEvaluator;
    private transient MethodHandle hashcodeHandle;

    private transient MethodHandle equalityHandle;

    public EqualityAndHashcodeProvider(
            SpecializedFunction.SpecializedContext context, DataType dataType) {
        hashcodeEvaluator =
                context.createEvaluator(
                        unresolvedCall(BuiltInFunctionDefinitions.INTERNAL_HASHCODE, $("element1")),
                        DataTypes.INT(),
                        DataTypes.FIELD("element1", dataType.notNull().toInternal()));

        equalityEvaluator =
                context.createEvaluator(
                        $("element1").isEqual($("element2")),
                        DataTypes.BOOLEAN(),
                        DataTypes.FIELD("element1", dataType.notNull().toInternal()),
                        DataTypes.FIELD("element2", dataType.notNull().toInternal()));
    }

    public void open(FunctionContext context) throws Exception {
        hashcodeHandle = hashcodeEvaluator.open(context);
        equalityHandle = equalityEvaluator.open(context);
    }

    public boolean equals(Object o1, Object o2) {
        try {
            return (Boolean) equalityHandle.invoke(o1, o2);
        } catch (Throwable e) {
            throw new FlinkRuntimeException(e);
        }
    }

    public int hashCode(Object o) {
        try {
            return (int) hashcodeHandle.invoke(o);
        } catch (Throwable e) {
            throw new FlinkRuntimeException(e);
        }
    }

    @Override
    public void close() throws IOException {
        hashcodeEvaluator.close();
        equalityEvaluator.close();
    }
}
