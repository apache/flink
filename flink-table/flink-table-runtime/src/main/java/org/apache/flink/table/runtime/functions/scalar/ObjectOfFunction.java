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
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.SpecializedFunction;

/** Implementation of {@link BuiltInFunctionDefinitions#OBJECT_OF}. */
@Internal
public class ObjectOfFunction extends BuiltInScalarFunction {

    public ObjectOfFunction(SpecializedFunction.SpecializedContext context) {
        super(BuiltInFunctionDefinitions.OBJECT_OF, context);
    }

    public RowData eval(StringData className, Object... fieldNameAndValuePairs) {
        final int fieldCount = fieldNameAndValuePairs.length / 2;
        final GenericRowData row = new GenericRowData(fieldCount);

        for (int i = 0; i < fieldCount; i++) {
            Object fieldValues = fieldNameAndValuePairs[2 * i + 1];
            row.setField(i, fieldValues);
        }

        return row;
    }
}
