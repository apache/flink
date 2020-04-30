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

package org.apache.flink.table.types.inference.transforms;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.TypeTransformation;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LegacyTypeInformationType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

/**
 * This type transformation transforms the legacy decimal type (usually converted from
 * {@link org.apache.flink.api.common.typeinfo.Types#BIG_DEC}) to DECIMAL(38, 18).
 */
public class LegacyDecimalTypeTransformation implements TypeTransformation {

	public static final TypeTransformation INSTANCE = new LegacyDecimalTypeTransformation();

	@Override
	public DataType transform(DataType typeToTransform) {
		LogicalType logicalType = typeToTransform.getLogicalType();
		if (logicalType instanceof LegacyTypeInformationType && logicalType.getTypeRoot() == LogicalTypeRoot.DECIMAL) {
			DataType decimalType = DataTypes
				.DECIMAL(DecimalType.MAX_PRECISION, 18)
				.bridgedTo(typeToTransform.getConversionClass());
			return logicalType.isNullable() ? decimalType : decimalType.notNull();
		}
		return typeToTransform;
	}
}
