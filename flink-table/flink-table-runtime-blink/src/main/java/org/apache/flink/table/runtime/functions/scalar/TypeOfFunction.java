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
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.SpecializedFunction.SpecializedContext;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.logical.LogicalType;

import javax.annotation.Nullable;

import java.util.List;

/** Implementation of {@link BuiltInFunctionDefinitions#TYPE_OF}. */
@Internal
public class TypeOfFunction extends BuiltInScalarFunction {

    private final String typeString;

    private transient StringData typeStringData;

    public TypeOfFunction(SpecializedContext context) {
        super(BuiltInFunctionDefinitions.TYPE_OF, context);
        final CallContext callContext = context.getCallContext();
        this.typeString = createTypeString(callContext, isForceSerializable(callContext));
    }

    // --------------------------------------------------------------------------------------------
    // Planning
    // --------------------------------------------------------------------------------------------

    private static boolean isForceSerializable(CallContext context) {
        final List<DataType> argumentDataTypes = context.getArgumentDataTypes();
        if (argumentDataTypes.size() != 2) {
            return false;
        }
        return context.getArgumentValue(1, Boolean.class).orElse(false);
    }

    private static @Nullable String createTypeString(
            CallContext context, boolean forceSerializable) {
        final LogicalType input = context.getArgumentDataTypes().get(0).getLogicalType();
        if (forceSerializable) {
            try {
                return input.asSerializableString();
            } catch (Exception t) {
                return null;
            }
        }
        return input.asSummaryString();
    }

    // --------------------------------------------------------------------------------------------
    // Runtime
    // --------------------------------------------------------------------------------------------

    @Override
    public void open(FunctionContext context) throws Exception {
        this.typeStringData = StringData.fromString(typeString);
    }

    @SuppressWarnings("unused")
    public @Nullable StringData eval(Object... unused) {
        return typeStringData;
    }
}
