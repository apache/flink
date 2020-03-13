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

package org.apache.flink.table.runtime.arrow.writers;

import org.apache.flink.annotation.Internal;
import org.apache.flink.types.Row;

import org.apache.arrow.vector.DecimalVector;

import java.math.BigDecimal;

import static org.apache.flink.table.runtime.typeutils.PythonTypeUtils.fromBigDecimal;

/**
 * {@link ArrowFieldWriter} for Decimal.
 */
@Internal
public final class DecimalWriter extends ArrowFieldWriter<Row> {

	private final int precision;
	private final int scale;

	public DecimalWriter(DecimalVector decimalVector, int precision, int scale) {
		super(decimalVector);
		this.precision = precision;
		this.scale = scale;
	}

	@Override
	public void doWrite(Row value, int ordinal) {
		if (value.getField(ordinal) == null) {
			((DecimalVector) getValueVector()).setNull(getCount());
		} else {
			BigDecimal bigDecimal = (BigDecimal) value.getField(ordinal);
			bigDecimal = fromBigDecimal(bigDecimal, precision, scale);
			if (bigDecimal == null) {
				((DecimalVector) getValueVector()).setNull(getCount());
			} else {
				((DecimalVector) getValueVector()).setSafe(getCount(), bigDecimal);
			}
		}
	}
}
