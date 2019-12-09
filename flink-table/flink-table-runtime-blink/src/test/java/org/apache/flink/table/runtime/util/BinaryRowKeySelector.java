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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.table.dataformat.BinaryRowWriter;
import org.apache.flink.table.dataformat.BinaryWriter;
import org.apache.flink.table.dataformat.TypeGetterSetters;
import org.apache.flink.table.runtime.keyselector.BaseRowKeySelector;
import org.apache.flink.table.runtime.types.InternalSerializers;
import org.apache.flink.table.runtime.typeutils.BaseRowTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;

/**
 * A utility class which extracts key from BaseRow.
 */
public class BinaryRowKeySelector implements BaseRowKeySelector {

	private static final long serialVersionUID = -2327761762415377059L;

	private final int[] keyFields;
	private final LogicalType[] inputFieldTypes;
	private final LogicalType[] keyFieldTypes;
	private final TypeSerializer[] keySers;

	public BinaryRowKeySelector(int[] keyFields, LogicalType[] inputFieldTypes) {
		this.keyFields = keyFields;
		this.inputFieldTypes = inputFieldTypes;
		this.keyFieldTypes = new LogicalType[keyFields.length];
		this.keySers = new TypeSerializer[keyFields.length];
		ExecutionConfig conf = new ExecutionConfig();
		for (int i = 0; i < keyFields.length; ++i) {
			keyFieldTypes[i] = inputFieldTypes[keyFields[i]];
			keySers[i] = InternalSerializers.create(keyFieldTypes[i], conf);
		}
	}

	@Override
	public BaseRow getKey(BaseRow value) throws Exception {
		BinaryRow ret = new BinaryRow(keyFields.length);
		BinaryRowWriter writer = new BinaryRowWriter(ret);
		for (int i = 0; i < keyFields.length; i++) {
			if (value.isNullAt(i)) {
				writer.setNullAt(i);
			} else {
				BinaryWriter.write(
						writer,
						i,
						TypeGetterSetters.get(value, keyFields[i], inputFieldTypes[keyFields[i]]),
						inputFieldTypes[keyFields[i]],
						keySers[i]);
			}
		}
		writer.complete();
		return ret;
	}

	@Override
	public BaseRowTypeInfo getProducedType() {
		return new BaseRowTypeInfo(keyFieldTypes);
	}
}
