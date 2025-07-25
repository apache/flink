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
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.SpecializedFunction;
import org.apache.flink.table.functions.SpecializedFunction.SpecializedContext;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.table.types.logical.StructuredType.StructuredAttribute;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

/** Implementation of {@link BuiltInFunctionDefinitions#OBJECT_UPDATE}. */
@Internal
public class ObjectUpdateFunction extends BuiltInScalarFunction {

    private Map<String, Integer> fieldNameToRowPosIndex = new HashMap<>();

    public ObjectUpdateFunction(SpecializedFunction.SpecializedContext context) {
        super(BuiltInFunctionDefinitions.OBJECT_UPDATE, context);
        this.fieldNameToRowPosIndex = createIndex(context);
    }

    public RowData eval(RowData rowData, Object... fieldNameAndValuePairs) {
        if (rowData == null) {
            return null;
        }

        GenericRowData updatedRow = (GenericRowData) rowData;

        for (int i = 0; i < fieldNameAndValuePairs.length; i += 2) {
            final String fieldName = fieldNameAndValuePairs[i].toString();
            final int position = fieldNameToRowPosIndex.get(fieldName);
            final Object fieldValue = fieldNameAndValuePairs[i + 1];
            updatedRow.setField(position, fieldValue);
        }

        return updatedRow;
    }

    private Map<String, Integer> createIndex(final SpecializedContext context) {
        StructuredType structuredType =
                (StructuredType)
                        context.getCallContext().getArgumentDataTypes().get(0).getLogicalType();
        final List<StructuredAttribute> attributes = structuredType.getAttributes();

        Map<String, Integer> fieldToPosIndex = new HashMap<>();

        IntStream.range(0, attributes.size())
                .forEach(pos -> fieldToPosIndex.put(attributes.get(pos).getName(), pos));

        return fieldToPosIndex;
    }
}
