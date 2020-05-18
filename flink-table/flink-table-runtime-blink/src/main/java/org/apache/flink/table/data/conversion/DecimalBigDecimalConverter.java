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

package org.apache.flink.table.data.conversion;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DecimalType;

import java.math.BigDecimal;

/**
 * Converter for {@link DecimalType} of {@link BigDecimal} external type.
 */
@Internal
public class DecimalBigDecimalConverter implements DataStructureConverter<DecimalData, BigDecimal> {

	private static final long serialVersionUID = 1L;

	private final int precision;

	private final int scale;

	private DecimalBigDecimalConverter(int precision, int scale) {
		this.precision = precision;
		this.scale = scale;
	}

	@Override
	public DecimalData toInternal(BigDecimal external) {
		return DecimalData.fromBigDecimal(external, precision, scale);
	}

	@Override
	public BigDecimal toExternal(DecimalData internal) {
		return internal.toBigDecimal();
	}

	// --------------------------------------------------------------------------------------------
	// Factory method
	// --------------------------------------------------------------------------------------------

	static DecimalBigDecimalConverter create(DataType dataType) {
		final DecimalType decimalType = (DecimalType) dataType.getLogicalType();
		return new DecimalBigDecimalConverter(decimalType.getPrecision(), decimalType.getScale());
	}
}
