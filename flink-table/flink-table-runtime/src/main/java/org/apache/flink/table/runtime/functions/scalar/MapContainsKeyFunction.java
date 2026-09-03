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
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.SpecializedFunction.ExpressionEvaluator;
import org.apache.flink.table.functions.SpecializedFunction.SpecializedContext;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.KeyValueDataType;
import org.apache.flink.util.FlinkRuntimeException;

import javax.annotation.Nullable;

import java.lang.invoke.MethodHandle;

import static org.apache.flink.table.api.Expressions.$;

/** Implementation of {@link BuiltInFunctionDefinitions#MAP_CONTAINS_KEY}. */
@Internal
public class MapContainsKeyFunction extends BuiltInScalarFunction {

    private final ArrayData.ElementGetter keyElementGetter;
    private final ExpressionEvaluator equalityEvaluator;
    private transient MethodHandle equalityHandle;

    public MapContainsKeyFunction(SpecializedContext context) {
        super(BuiltInFunctionDefinitions.MAP_CONTAINS_KEY, context);
        final DataType mapDataType = context.getCallContext().getArgumentDataTypes().get(0);
        final DataType keyDataType = ((KeyValueDataType) mapDataType).getKeyDataType();

        keyElementGetter = ArrayData.createElementGetter(keyDataType.getLogicalType());
        equalityEvaluator =
                context.createEvaluator(
                        $("key").isEqual($("needle")),
                        DataTypes.BOOLEAN(),
                        DataTypes.FIELD("key", keyDataType.notNull().toInternal()),
                        DataTypes.FIELD("needle", keyDataType.notNull().toInternal()));
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        equalityHandle = equalityEvaluator.open(context);
    }

    public @Nullable Boolean eval(@Nullable MapData map, @Nullable Object needle) {
        if (map == null) {
            return null;
        }
        final ArrayData keys = map.keyArray();
        final int size = map.size();
        for (int pos = 0; pos < size; pos++) {
            final Object elementKey = keyElementGetter.getElementOrNull(keys, pos);
            // A NULL needle matches a NULL key, unlike SQL `NULL = NULL` which yields UNKNOWN.
            if (needle == null && elementKey == null) {
                return true;
            } else if (needle != null && elementKey != null && isEqual(elementKey, needle)) {
                return true;
            }
        }
        return false;
    }

    private boolean isEqual(final Object key, final Object needle) {
        try {
            return (boolean) equalityHandle.invoke(key, needle);
        } catch (Throwable t) {
            throw new FlinkRuntimeException(t);
        }
    }

    @Override
    public void close() throws Exception {
        equalityEvaluator.close();
    }
}
