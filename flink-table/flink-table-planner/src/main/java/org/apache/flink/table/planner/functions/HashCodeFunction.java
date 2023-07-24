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

package org.apache.flink.table.planner.functions;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.SpecializedFunction;
import org.apache.flink.table.runtime.functions.scalar.BuiltInScalarFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.FlinkRuntimeException;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

/** Implementation of {@link BuiltInFunctionDefinitions#HASHCODE}. */
@Internal
public class HashCodeFunction extends BuiltInScalarFunction {
    private final List<DataType> copyOfDataType;

    public HashCodeFunction(SpecializedFunction.SpecializedContext context) {
        super(BuiltInFunctionDefinitions.HASHCODE, context);
        List<DataType> dataType = context.getCallContext().getArgumentDataTypes();
        copyOfDataType = new ArrayList<>();
        for (int i = 0; i < dataType.size(); ++i) {
            copyOfDataType.add(dataType.get(i));
        }
    }

    public @Nullable Boolean eval(RowData o1, RowData o2) {
        try {
            if (o1 == null && o2 == null) {
                return true;
            }
            if (o1 == null || o2 == null) {
                return false;
            }
            boolean isEqual = checkHashCode(o1, o2);
            if (isEqual) {
                return true;
            }
            return false;
        } catch (Throwable t) {
            throw new FlinkRuntimeException(t);
        }
    }

    private boolean checkHashCode(RowData o1, RowData o2) {
        GetHashCodeGenerator getHashCodeGenerator = new GetHashCodeGenerator();
        int n = o1.getArity();
        RowData.FieldGetter fieldGetter = null;
        List<LogicalType> logicalTypeList = new ArrayList<>();
        List<Object> objectList = new ArrayList<>();
        for (int i = 0; i < n; ++i) {
            fieldGetter =
                    RowData.createFieldGetter(
                            copyOfDataType.get(0).getChildren().get(i).getLogicalType(), i);
            logicalTypeList.add(copyOfDataType.get(0).getChildren().get(i).getLogicalType());
            objectList.add(fieldGetter.getFieldOrNull(o1));
        }
        int n2 = o2.getArity();
        RowData.FieldGetter fieldGetter2 = null;
        List<LogicalType> logicalTypeList2 = new ArrayList<>();
        List<Object> objectList2 = new ArrayList<>();
        for (int i = 0; i < n2; ++i) {
            fieldGetter2 =
                    RowData.createFieldGetter(
                            copyOfDataType.get(1).getChildren().get(i).getLogicalType(), i);
            logicalTypeList2.add(copyOfDataType.get(1).getChildren().get(i).getLogicalType());
            objectList2.add(fieldGetter2.getFieldOrNull(o2));
        }
        ClassLoader classLoader;
        classLoader = Thread.currentThread().getContextClassLoader();
        int hashCode1 = getHashCodeGenerator.getHashCode(logicalTypeList, o1, classLoader);
        int hashCode2 = getHashCodeGenerator.getHashCode(logicalTypeList2, o2, classLoader);

        if (hashCode1 == hashCode2) {
            return true;
        }
        return false;
    }

    public List<LogicalType> getLogicalType(GenericRowData rowData) {
        if (rowData == null) {
            return null;
        }
        if (rowData.getArity() == 0) {
            return new ArrayList<>();
        }
        return null;
    }
}
