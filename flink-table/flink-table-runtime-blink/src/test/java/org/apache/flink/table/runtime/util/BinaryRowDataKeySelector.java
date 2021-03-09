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

package org.apache.flink.table.runtime.util;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.writer.BinaryRowWriter;
import org.apache.flink.table.data.writer.BinaryWriter;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;

/** A utility class which extracts key from RowData. */
public class BinaryRowDataKeySelector implements RowDataKeySelector {

    private static final long serialVersionUID = -2327761762415377059L;

    private final int[] keyFields;
    private final LogicalType[] inputFieldTypes;
    private final LogicalType[] keyFieldTypes;
    private final TypeSerializer[] keySers;
    private final RowData.FieldGetter[] fieldGetters;

    public BinaryRowDataKeySelector(int[] keyFields, LogicalType[] inputFieldTypes) {
        this.keyFields = keyFields;
        this.inputFieldTypes = inputFieldTypes;
        this.keyFieldTypes = new LogicalType[keyFields.length];
        this.keySers = new TypeSerializer[keyFields.length];
        this.fieldGetters = new RowData.FieldGetter[keyFields.length];
        for (int i = 0; i < keyFields.length; ++i) {
            keyFieldTypes[i] = inputFieldTypes[keyFields[i]];
            keySers[i] = InternalSerializers.create(keyFieldTypes[i]);
            fieldGetters[i] =
                    RowData.createFieldGetter(inputFieldTypes[keyFields[i]], keyFields[i]);
        }
    }

    @Override
    public RowData getKey(RowData value) throws Exception {
        BinaryRowData ret = new BinaryRowData(keyFields.length);
        BinaryRowWriter writer = new BinaryRowWriter(ret);
        for (int i = 0; i < keyFields.length; i++) {
            if (value.isNullAt(i)) {
                writer.setNullAt(i);
            } else {
                BinaryWriter.write(
                        writer,
                        i,
                        fieldGetters[i].getFieldOrNull(value),
                        inputFieldTypes[keyFields[i]],
                        keySers[i]);
            }
        }
        writer.complete();
        return ret;
    }

    @Override
    public InternalTypeInfo<RowData> getProducedType() {
        return InternalTypeInfo.ofFields(keyFieldTypes);
    }
}
