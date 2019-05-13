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

package org.apache.flink.table.plan.util;

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.codegen.CodeGeneratorContext;
import org.apache.flink.table.codegen.ProjectionCodeGenerator;
import org.apache.flink.table.generated.GeneratedProjection;
import org.apache.flink.table.runtime.keyselector.BaseRowKeySelector;
import org.apache.flink.table.runtime.keyselector.BinaryRowKeySelector;
import org.apache.flink.table.runtime.keyselector.NullBinaryRowKeySelector;
import org.apache.flink.table.type.InternalType;
import org.apache.flink.table.type.RowType;
import org.apache.flink.table.typeutils.BaseRowSerializer;
import org.apache.flink.table.typeutils.BaseRowTypeInfo;
import org.apache.flink.table.typeutils.TypeCheckUtils;

/**
 * Utility for KeySelector.
 */
public class KeySelectorUtil {

	/**
	 * Create a BaseRowKeySelector to extract keys from DataStream which type is BaseRowTypeInfo.
	 *
	 * @param keyFields key fields
	 * @param rowType type of DataStream to extract keys
	 * @return the BaseRowKeySelector to extract keys from DataStream which type is BaseRowTypeInfo.
	 */
	public static BaseRowKeySelector getBaseRowSelector(int[] keyFields, BaseRowTypeInfo rowType) {
		if (keyFields.length > 0) {
			InternalType[] inputFieldTypes = rowType.getInternalTypes();
			String[] inputFieldNames = rowType.getFieldNames();
			InternalType[] keyFieldTypes = new InternalType[keyFields.length];
			String[] keyFieldNames = new String[keyFields.length];
			for (int i = 0; i < keyFields.length; ++i) {
				keyFieldTypes[i] = inputFieldTypes[keyFields[i]];
				keyFieldNames[i] = inputFieldNames[keyFields[i]];
			}
			RowType returnType = new RowType(keyFieldTypes, keyFieldNames);
			RowType inputType = new RowType(inputFieldTypes, rowType.getFieldNames());
			GeneratedProjection generatedProjection = ProjectionCodeGenerator.generateProjection(
					CodeGeneratorContext.apply(new TableConfig()),
					BaseRowSerializer.class.getSimpleName(), inputType, returnType, keyFields);
			BaseRowTypeInfo keyRowType = returnType.toTypeInfo();
			// check if type implements proper equals/hashCode
			TypeCheckUtils.validateEqualsHashCode("grouping", keyRowType);
			return new BinaryRowKeySelector(keyRowType, generatedProjection);
		} else {
			return new NullBinaryRowKeySelector();
		}
	}

}
