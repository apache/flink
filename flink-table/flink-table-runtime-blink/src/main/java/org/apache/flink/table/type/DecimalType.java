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

package org.apache.flink.table.type;

import java.math.BigDecimal;

import static java.lang.String.format;

/**
 * Decimal type.
 */
public class DecimalType implements AtomicType {

	public static final int MAX_PRECISION = 38;

	private final int precision;
	private final int scale;

	// Default to be same with Integer.
	public static final DecimalType USER_DEFAULT = new DecimalType(10, 0);

	// Mainly used for implicitly type cast and test.
	public static final DecimalType SYSTEM_DEFAULT = new DecimalType(MAX_PRECISION, 18);

	public DecimalType(int precision, int scale) {
		if (precision < 0) {
			throw new IllegalArgumentException(format("Decimal precision (%s) cannot be negative.",
					precision));
		}

		if (scale > precision) {
			throw new IllegalArgumentException(format("Decimal scale (%s) cannot be greater than " +
					"precision (%s).", scale, precision));
		}

		if (precision > MAX_PRECISION) {
			throw new IllegalArgumentException(
					"DecimalType can only support precision up to " + MAX_PRECISION);
		}

		this.precision = precision;
		this.scale = scale;
	}

	public int precision() {
		return this.precision;
	}

	public int scale() {
		return this.scale;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		DecimalType that = (DecimalType) o;

		return precision == that.precision && scale == that.scale;
	}

	@Override
	public int hashCode() {
		int result = precision;
		result = 31 * result + scale;
		return result;
	}

	@Override
	public String toString() {
		return "DecimalType{" +
				"precision=" + precision +
				", scale=" + scale +
				'}';
	}

	public static DecimalType of(int precision, int scale) {
		return new DecimalType(precision, scale);
	}

	public static DecimalType of(BigDecimal value) {
		return of(value.precision(), value.scale());
	}
}
