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
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.SpecializedFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.FlinkRuntimeException;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

/** Implementation of {@link BuiltInFunctionDefinitions#ELT}. */
@Internal
public class EltFunction extends BuiltInScalarFunction {

    private final List<DataType> copyOfDataType;

    public EltFunction(SpecializedFunction.SpecializedContext context) {
        super(BuiltInFunctionDefinitions.ELT, context);
        List<DataType> dataType = context.getCallContext().getArgumentDataTypes();
        copyOfDataType = new ArrayList<>();
        for (int i = 0; i < dataType.size(); ++i) {
            copyOfDataType.add(dataType.get(i));
        }
    }

    public @Nullable Object eval(RowData inputs, Integer n) {
        try {
            if (n == null || inputs == null) {
                return null;
            }
            if (n < 1 || n > inputs.getArity()) {
                return null;
            }
            RowData.FieldGetter fieldGetter =
                    RowData.createFieldGetter(
                            copyOfDataType.get(0).getChildren().get(n - 1).getLogicalType(), n - 1);
            Object res = fieldGetter.getFieldOrNull(inputs);
            return res;
        } catch (Throwable t) {
            throw new FlinkRuntimeException(t);
        }
    }
}
